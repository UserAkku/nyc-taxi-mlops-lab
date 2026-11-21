"""S2's pipeline: the window, the gate, and training_set -> trained_model -> eval_report -> challenger end
to end against a fake Redshift (UNLOAD writes synthetic trips) and a local MLflow (SQLite)."""

import dagster as dg
import pytest
from fakes import LocalArtifacts, unloading_redshift
from mlflow import MlflowClient

from labpipes.contracts import mlflow_contract
from labpipes.defs import training
from labpipes.defs.training import challenger, eval_report, plan_window, promotion_verdict, trained_model, training_set, unload
from labpipes.resources import MlflowTracking

MONTHS = ["2024-01", "2024-02", "2024-03", "2024-04"]

# --- the window ---


def test_the_holdout_is_the_latest_month_and_the_window_is_the_three_before_it():
    assert plan_window(MONTHS, "", 3) == (["2024-01", "2024-02", "2024-03"], "2024-04")


def test_an_earlier_holdout_never_includes_itself_or_later_months_in_training():
    train, hold = plan_window(MONTHS, "2024-03", 2)
    assert (train, hold) == (["2024-01", "2024-02"], "2024-03")
    assert hold not in train and max(train) < hold


def test_too_few_earlier_months_and_unknown_holdouts_are_refused():
    with pytest.raises(ValueError, match="need 3 months before 2024-02"):
        plan_window(MONTHS, "2024-02", 3)
    with pytest.raises(ValueError, match="isn't in staging.trip_features"):
        plan_window(MONTHS, "2025-01", 3)
    with pytest.raises(ValueError, match="no months"):
        plan_window([], "", 3)
    with pytest.raises(ValueError):
        plan_window(MONTHS + ["2024-05'; drop table x; --"], "", 3)


def test_unload_doubles_the_quotes_inside_the_query():
    sql = unload("select a from t where m in ('2024-01')", "s3://b/p/")
    assert sql == "unload ('select a from t where m in (''2024-01'')') to 's3://b/p/' iam_role default format as parquet allowoverwrite"


# --- the gate ---

# decision 008's numbers are set in the test, not assumed: margin 5%, 100 ms
def test_the_gate_needs_the_margin_and_the_latency_budget():
    assert promotion_verdict(9.0, 5.0, 10.0, 0.05, 100) == (True, [])
    ok, why = promotion_verdict(9.6, 5.0, 10.0, 0.05, 100)  # 4% better is not enough for 5%
    assert not ok and "doesn't beat" in why[0]
    ok, why = promotion_verdict(9.0, 150.0, 10.0, 0.05, 100)  # accurate but too slow
    assert not ok and "budget" in why[0]
    ok, why = promotion_verdict(5.0, 150.0, 10.0, 0.05, 100)
    assert not ok and len(why) == 1


def test_an_undecided_gate_refuses_even_a_perfect_model():
    ok, why = promotion_verdict(0.0, 0.0, 10.0, None, None)
    assert not ok and "isn't decided" in why[0]


# --- end to end ---


@pytest.fixture
def stack(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # MLflow's default artifact location is ./mlruns
    monkeypatch.setattr(training, "NUM_ROUNDS", 40)
    uri = f"sqlite:///{tmp_path}/mlflow.db"
    root = str(tmp_path / "s3")
    resources = {"redshift": unloading_redshift(root), "artifacts": LocalArtifacts(bucket="lab", root=root),
                 "tracking": MlflowTracking(tracking_uri=uri)}
    return resources, MlflowClient(uri)


def run(resources):
    return dg.materialize([training_set, trained_model, eval_report, challenger], resources=resources, raise_on_error=False)


def beats_champion(result):
    (ev,) = [e for e in result.get_asset_check_evaluations() if e.check_name == "beats_champion"]
    return ev


def test_while_the_rule_is_undecided_a_model_is_registered_and_measured_but_never_the_challenger(stack):
    resources, client = stack
    result = run(resources)
    assert not result.success and not beats_champion(result).passed
    assert "isn't decided" in beats_champion(result).description
    assert result.output_for_node("trained_model")["version"] == "1"
    assert client.get_model_version("trip-duration", "1").tags["mae_min"]  # still measured and tagged
    with pytest.raises(Exception, match="alias"):
        client.get_model_version_by_alias("trip-duration", "challenger")


def test_a_model_that_beats_the_baseline_by_the_margin_becomes_the_challenger_with_every_tag(stack, monkeypatch):
    resources, client = stack
    monkeypatch.setattr(training, "PROMOTION_MARGIN", 0.05)
    monkeypatch.setattr(training, "LATENCY_BUDGET_MS", 5000.0)
    result = run(resources)
    assert result.success and beats_champion(result).passed

    report = result.output_for_node("eval_report")
    assert report["compared_with"] == "the v1 baseline (no champion yet)"
    assert report["metrics"]["mae_min"] < report["metrics"]["champion_mae_min"]
    assert result.output_for_node("training_set")["window"] == "2024-01..2024-03"
    assert result.output_for_node("training_set")["holdout_month"] == "2024-04"

    contract = mlflow_contract()
    mv = client.get_model_version_by_alias(contract["registered_model"], "challenger")
    assert str(mv.version) == "1"
    assert set(contract["version_tags"]) <= set(mv.tags), set(contract["version_tags"]) - set(mv.tags)
    assert mv.tags["training_window"] == "2024-01..2024-03" and mv.tags["holdout_month"] == "2024-04"
    assert mv.tags["features_version"] == "1"
    run_metrics = client.get_run(mv.run_id).data.metrics
    assert set(contract["metrics"]) <= set(run_metrics) | {"mae_min.airport", "mae_min.manhattan", "mae_min.outer", "mae_min.night"}


def test_the_pipeline_never_sets_champion(stack, monkeypatch):
    resources, client = stack
    monkeypatch.setattr(training, "PROMOTION_MARGIN", 0.05)
    monkeypatch.setattr(training, "LATENCY_BUDGET_MS", 5000.0)
    run(resources)
    with pytest.raises(Exception, match="alias"):
        client.get_model_version_by_alias("trip-duration", "champion")


def test_a_retrain_that_only_ties_the_champion_does_not_move_the_challenger(stack, monkeypatch):
    resources, client = stack
    monkeypatch.setattr(training, "PROMOTION_MARGIN", 0.05)
    monkeypatch.setattr(training, "LATENCY_BUDGET_MS", 5000.0)
    assert run(resources).success
    client.set_registered_model_alias("trip-duration", "champion", "1")  # what S3's approved promotion does

    second = run(resources)  # the same data, so the same quality: it can't beat the champion by 5%
    assert not second.success and not beats_champion(second).passed
    assert second.output_for_node("eval_report")["compared_with"] == "champion v1"
    assert str(client.get_model_version_by_alias("trip-duration", "challenger").version) == "1"  # not moved to v2
    assert str(client.get_model_version_by_alias("trip-duration", "champion").version) == "1"


def test_challenger_on_its_own_cannot_skip_the_gate(stack):
    resources, client = stack
    bad = {"name": "trip-duration", "version": "1", "metrics": {"mae_min": 9.0, "p95_latency_ms": 1.0, "champion_mae_min": 10.0}}
    with pytest.raises(dg.Failure, match="not promoted"):
        challenger(bad, resources["tracking"])  # undecided rule: refused although 9 < 10
