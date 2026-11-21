"""S2: training_set, trained_model, eval_report, the challenger gate and the challenger asset.

- training_set: UNLOAD a window of staging.trip_features (and the holdout month) to the artifact bucket.
  The zone-pair distance lookup (contracts/features.yaml, est_distance_miles) comes from the window only.
- trained_model: fit tripmodel.LightGBMModel (the given v2), log it to MLflow as a pyfunc with
  tripmodel.model_signature() and the tripmodel package bundled, register a version of the model named
  in contracts/mlflow.yaml, and tag it as that contract says.
- eval_report: tripmodel.evaluate the new version and the champion (or the v1 baseline before there is
  one) on the same holdout; log the metrics.
- A blocking check that guards a `challenger` asset, which sets the alias. The pipeline never moves
  `champion`.
Decision points: what stays in the notebook; the promotion metric and margin; the training window.

Decisions (decisions/007, 008, 009):
- Window: the WINDOW_MONTHS months before the holdout train; the holdout is the latest month in
  staging.trip_features (2024-04, trained on 2024-01..03). The holdout is always after the window.
- Promotion: a new version must beat what it is compared against on mae_min by PROMOTION_MARGIN (2%) and
  stay within LATENCY_BUDGET_MS (25 ms p95). If either were None the gate would fail closed: eval_report
  still measures and logs, but nothing becomes `challenger`.
- The notebook explores; everything that registers, gates or aliases is here.

Data flow: staging.trip_features --UNLOAD--> s3://<artifact bucket>/training_sets/<window>__<holdout>/
{train,holdout,distances}/ --> fit --> MLflow (registered version + tags) --> eval_report (metrics, gate)
--> challenger (alias). Small dicts pass between the assets (Dagster's default IO manager); the data
itself stays in S3, never in a pickle.
"""

import os
from pathlib import Path

import dagster as dg
import mlflow
import tripmodel
from tripmodel import BaselineModel, LightGBMModel, evaluate, model_signature

from ..contracts import features_contract, mlflow_contract
from ..partitions import check_month
from ..resources import ArtifactBucket, MlflowTracking, RedshiftData

WINDOW_MONTHS = 3  # decision 009: train on this many months, test on the month after
NUM_ROUNDS = 300  # LightGBM boosting rounds (tripmodel's own defaults for everything else)

# Decision 008 (promotion metric and margin). A new version must have
# mae_min <= champion_mae_min * (1 - PROMOTION_MARGIN) and p95_latency_ms <= LATENCY_BUDGET_MS.
# None would mean "not decided": the gate then refuses everything (fail closed).
# Measured on the first run (train 2024-01..03, test 2024-04): v1 baseline 3.956 min, v2 3.637 min (8.1%
# better), v2 p95 5.4 ms on the platform host.
PROMOTION_MARGIN: float | None = 0.02
LATENCY_BUDGET_MS: float | None = 25.0

FEATURE_COLUMNS = "pickup_zone_id, dropoff_zone_id, pickup_hour, pickup_dow, duration_min"
CHALLENGER = "challenger"


# --- the window ---


def plan_window(available: list[str], holdout: str, window_months: int) -> tuple[list[str], str]:
    """(training months, holdout month): the holdout is the one given, else the latest available month, and
    the training months are the `window_months` available months right before it (never including it)."""
    months = sorted(check_month(m) for m in available)
    if not months:
        raise ValueError("staging.trip_features has no months: ingest and build the features first")
    hold = check_month(holdout) if holdout else months[-1]
    if hold not in months:
        raise ValueError(f"holdout {hold} isn't in staging.trip_features ({months[0]}..{months[-1]})")
    earlier = [m for m in months if m < hold]
    if len(earlier) < window_months:
        raise ValueError(f"need {window_months} months before {hold}, found {len(earlier)} ({', '.join(earlier) or 'none'})")
    return earlier[-window_months:], hold


def months_sql(months: list[str]) -> str:
    """'2024-01', '2024-02' for use inside a query (each month validated)."""
    return ", ".join(f"'{check_month(m)}'" for m in months)


def unload(select: str, uri: str) -> str:
    """UNLOAD a query to S3 as Parquet (single quotes in the query are doubled, as UNLOAD requires)."""
    return f"unload ('{select.replace(chr(39), chr(39) * 2)}') to '{uri}' iam_role default format as parquet allowoverwrite"


class TrainConfig(dg.Config):
    holdout_month: str = ""  # YYYY-MM; empty = the latest month in staging.trip_features
    window_months: int = WINDOW_MONTHS


@dg.asset(deps=["trip_features"], group_name="model", kinds={"redshift", "s3"})
def training_set(config: TrainConfig, redshift: RedshiftData, artifacts: ArtifactBucket) -> dg.Output:
    available = [r["trip_month"] for r in redshift.query("select distinct trip_month from staging.trip_features order by 1")]
    train, holdout = plan_window(available, config.holdout_month, config.window_months)
    window = f"{train[0]}..{train[-1]}"
    prefix = f"training_sets/{window}__{holdout}/"

    counts = redshift.query(
        f"select (select count(*) from staging.trip_features where trip_month in ({months_sql(train)})) as train, "
        f"(select count(*) from staging.trip_features where trip_month = '{holdout}') as holdout"
    )[0]
    # Start from an empty prefix: UNLOAD's file count varies, and a stale part file would be read as data.
    artifacts.filesystem().delete_dir_contents(f"{artifacts.bucket}/{prefix}", missing_dir_ok=True)
    redshift.execute(unload(
        f"select {FEATURE_COLUMNS} from staging.trip_features where trip_month in ({months_sql(train)})", artifacts.uri(prefix + "train/")))
    redshift.execute(unload(
        f"select {FEATURE_COLUMNS} from staging.trip_features where trip_month = '{holdout}'", artifacts.uri(prefix + "holdout/")))
    # The distance lookup is a window-only statistic: the holdout's trips never feed it.
    redshift.execute(unload(
        "select pickup_zone_id, dropoff_zone_id, median(trip_distance) as est_distance_miles from staging.stg_trips "
        f"where trip_month in ({months_sql(train)}) and duration_min > 0 and duration_min <= 180 and trip_distance > 0 "
        "and pickup_zone_id between 1 and 263 and dropoff_zone_id between 1 and 263 group by 1, 2",
        artifacts.uri(prefix + "distances/")))

    value = {"window": window, "train_months": train, "holdout_month": holdout, "prefix": prefix,
             "train_rows": int(counts["train"]), "holdout_rows": int(counts["holdout"])}
    return dg.Output(value, metadata={
        "window": window, "holdout_month": holdout, "train_rows": dg.MetadataValue.int(value["train_rows"]),
        "holdout_rows": dg.MetadataValue.int(value["holdout_rows"]), "uri": artifacts.uri(prefix)})


# --- training and registration ---


def tags_for(contract: dict, window: str, holdout: str) -> dict[str, str]:
    """The version tags known at training time (the metric tags are added by eval_report)."""
    return {"training_window": window, "holdout_month": holdout,
            "features_version": str(features_contract()["version"]), "git_sha": os.environ.get("GIT_SHA", "unknown")}


@dg.asset(group_name="model", kinds={"mlflow", "lightgbm"})
def trained_model(training_set: dict, artifacts: ArtifactBucket, tracking: MlflowTracking) -> dg.Output:
    contract = mlflow_contract()
    train = artifacts.read_parquet(training_set["prefix"] + "train")
    distances = artifacts.read_parquet(training_set["prefix"] + "distances")
    model = LightGBMModel().fit(train, distances, num_rounds=NUM_ROUNDS)
    del train

    client = tracking.client()  # also points mlflow at the server and selects the contract's experiment
    with mlflow.start_run(run_name=f"train {training_set['window']}") as run:
        mlflow.log_params({"training_window": training_set["window"], "holdout_month": training_set["holdout_month"],
                           "train_rows": training_set["train_rows"], "num_rounds": NUM_ROUNDS, **{f"lgb.{k}": v for k, v in model.params.items()}})
        info = mlflow.pyfunc.log_model(
            name="model", python_model=model, signature=model_signature(),
            code_paths=[str(Path(tripmodel.__file__).parent)],  # loading needs no pipeline code
            registered_model_name=contract["registered_model"])
    version = str(info.registered_model_version)
    for k, v in tags_for(contract, training_set["window"], training_set["holdout_month"]).items():
        client.set_model_version_tag(contract["registered_model"], version, k, v)
    value = {"name": contract["registered_model"], "version": version, "run_id": run.info.run_id}
    return dg.Output(value, metadata={"model": f"{value['name']} v{version}", "run_id": run.info.run_id})


# --- evaluation and the gate ---


def promotion_verdict(mae: float, p95_ms: float, champion_mae: float, margin: float | None, budget_ms: float | None) -> tuple[bool, list[str]]:
    """Does a new version beat what it's compared against, by the margin, within the latency budget?
    Fails closed while the rule is undecided. Returns (passed, reasons it didn't)."""
    if margin is None or budget_ms is None:
        return False, ["the promotion rule isn't decided yet (decisions/008-promotion-metric-and-margin.md): nothing is promoted"]
    reasons = []
    needed = champion_mae * (1 - margin)
    if not mae <= needed:
        reasons.append(f"mae_min {mae:.3f} doesn't beat {champion_mae:.3f} by {margin:.1%} (needs <= {needed:.3f})")
    if not p95_ms <= budget_ms:
        reasons.append(f"p95_latency_ms {p95_ms:.1f} is over the {budget_ms:g} ms budget")
    return not reasons, reasons


def zone_boroughs(redshift: RedshiftData) -> dict[int, str]:
    return {int(r["location_id"]): r["borough"] for r in redshift.query("select location_id, borough from ingest.taxi_zones")}


@dg.asset(
    group_name="model", kinds={"mlflow"},
    check_specs=[dg.AssetCheckSpec(name="beats_champion", asset="eval_report", blocking=True,
                                   description="The new version beats the champion (or the v1 baseline) by the margin, within the latency budget.")],
)
def eval_report(trained_model: dict, training_set: dict, redshift: RedshiftData, artifacts: ArtifactBucket, tracking: MlflowTracking):
    contract = mlflow_contract()
    name, version = trained_model["name"], trained_model["version"]
    client = tracking.client()
    holdout = artifacts.read_parquet(training_set["prefix"] + "holdout")
    boroughs = zone_boroughs(redshift)

    new = evaluate(mlflow.pyfunc.load_model(f"models:/{name}/{version}").predict, holdout, boroughs)

    try:  # before S3's promotion workflow has set one there is no champion: compare with v1 instead
        champ_version = client.get_model_version_by_alias(name, "champion").version
    except Exception:
        champ_version = None
    if champ_version:
        champion = evaluate(mlflow.pyfunc.load_model(f"models:/{name}@champion").predict, holdout, boroughs, latency_samples=20)
        compared = f"champion v{champ_version}"
    else:
        train = artifacts.read_parquet(training_set["prefix"] + "train")
        baseline = BaselineModel().fit(train, artifacts.read_parquet(training_set["prefix"] + "distances"))
        del train
        champion = evaluate(lambda df: baseline.predict(None, df), holdout, boroughs, latency_samples=20)
        compared = "the v1 baseline (no champion yet)"
    metrics = {**{k: v for k, v in new.items() if k != "holdout_rows"}, "champion_mae_min": champion["mae_min"]}

    for key in contract["metrics"]:  # on the training run, and as tags on the version (contracts/mlflow.yaml)
        if key in metrics:
            client.log_metric(trained_model["run_id"], key, metrics[key])
            client.set_model_version_tag(name, version, key, f"{metrics[key]:.4f}")

    passed, reasons = promotion_verdict(metrics["mae_min"], metrics["p95_latency_ms"], metrics["champion_mae_min"], PROMOTION_MARGIN, LATENCY_BUDGET_MS)
    report = {**trained_model, "compared_with": compared, "metrics": metrics, "passed": passed, "reasons": reasons}
    yield dg.Output(report, metadata={
        "model": f"{name} v{version}", "compared_with": compared, "mae_min": metrics["mae_min"],
        "champion_mae_min": metrics["champion_mae_min"], "p95_latency_ms": metrics["p95_latency_ms"],
        "holdout_rows": dg.MetadataValue.int(int(new["holdout_rows"])),
        "slices": dg.MetadataValue.json({k: v for k, v in metrics.items() if k.startswith("mae_min.")})})
    yield dg.AssetCheckResult(
        check_name="beats_champion", passed=passed, severity=dg.AssetCheckSeverity.ERROR,
        description="beats it" if passed else "; ".join(reasons),
        metadata={"mae_min": metrics["mae_min"], "champion_mae_min": metrics["champion_mae_min"], "compared_with": compared})


@dg.asset(group_name="model", kinds={"mlflow"})
def challenger(eval_report: dict, tracking: MlflowTracking) -> dg.MaterializeResult:
    """Sets the `challenger` alias, and only that alias. It runs after the blocking beats_champion check;
    it re-checks the verdict itself so that materializing it on its own can't skip the gate."""
    m = eval_report["metrics"]
    passed, reasons = promotion_verdict(m["mae_min"], m["p95_latency_ms"], m["champion_mae_min"], PROMOTION_MARGIN, LATENCY_BUDGET_MS)
    if not passed:
        raise dg.Failure(description="not promoted: " + "; ".join(reasons))
    client = tracking.client()
    client.set_registered_model_alias(eval_report["name"], CHALLENGER, eval_report["version"])
    return dg.MaterializeResult(metadata={"alias": CHALLENGER, "model": f"{eval_report['name']} v{eval_report['version']}"})


train_model = dg.define_asset_job(
    "train_model",
    selection=dg.AssetSelection.assets("training_set", "trained_model", "eval_report", "challenger"),
    description="Window to challenger: UNLOAD, fit, register, evaluate, gate, alias. Start it from the UI or scripts/dagster-run.sh.",
)
