"""S2: the feature_contract check on trip_features.

contracts/features.yaml lists the model's inputs, label and the post-trip columns that must never be
inputs; contracts/mlflow.yaml says this check blocks. Decide what "matches" means: column names,
types, value ranges. Gameday E1 will test it.

What "matches" means here, for the partition (month) being checked:

1. Columns: the table has exactly the contract's keys, inputs and label. A column that isn't in the
   contract fails, and one of the contract's `forbidden` columns (known only after a trip: distance, fare,
   dropoff time, ...) is named in the message, because that is target leakage: a model trained on it
   looks great on a holdout and can't be served, since a rider hasn't taken the trip yet.
2. Types: each column has the contract's type (a drifted type is a silent behaviour change).
3. Values, on this month's rows: no nulls, every input inside its range, the label inside (0, 180].
4. The month has rows at all.

Any problem blocks (contracts/mlflow.yaml): training reads these rows, so a bad month must not reach it.
"""

import re

import dagster as dg

from ..contracts import features_contract
from ..partitions import check_month, trip_months
from ..resources import RedshiftData
from .features import trip_features

# The contract's SQL type names as information_schema reports them in Redshift.
SQL_TYPES = {"char": "character", "integer": "integer", "smallint": "smallint", "double precision": "double precision"}


def _base(sql_type: str) -> str:
    return SQL_TYPES.get(re.sub(r"\(.*\)", "", sql_type).strip().lower(), sql_type.strip().lower())


def expected_columns(contract: dict) -> dict[str, str]:
    """Column name -> contract type: keys, inputs, then the label."""
    cols = [*contract["keys"], *contract["inputs"], contract["label"]]
    return {c["name"]: _base(c["type"]) for c in cols}


def column_problems(actual: list[tuple[str, str]], contract: dict) -> list[str]:
    """How the table's (name, type) columns differ from the contract; empty when they match."""
    want = expected_columns(contract)
    forbidden = set(contract["forbidden"])
    have = {name.lower(): _base(t) for name, t in actual}
    problems = []
    for name in sorted(set(have) - set(want)):
        if name in forbidden:
            problems.append(f"{name} is known only after the trip (target leakage) and must never be a column")
        else:
            problems.append(f"{name} is not in the contract")
    for name in sorted(set(want) - set(have)):
        problems.append(f"{name} is missing")
    for name in sorted(set(want) & set(have)):
        if want[name] != have[name]:
            problems.append(f"{name} is {have[name]}, the contract says {want[name]}")
    return problems


def value_conditions(contract: dict) -> dict[str, str]:
    """name -> SQL condition that is true for a row whose value breaks the contract."""
    out = {}
    for c in contract["inputs"]:
        lo, hi = c["range"]
        out[c["name"]] = f"{c['name']} is null or {c['name']} < {lo} or {c['name']} > {hi}"
    label = contract["label"]
    lo, hi = label["range"]
    low = f"{label['name']} <= {lo}" if label.get("exclusive_min") else f"{label['name']} < {lo}"
    out[label["name"]] = f"{label['name']} is null or {low} or {label['name']} > {hi}"
    return out


def value_problems(bad: dict[str, int], rows: int) -> list[str]:
    """Out-of-range counts and an empty month, as sentences."""
    problems = [f"{n} rows have {name} null or out of range" for name, n in bad.items() if n]
    if rows == 0:
        problems.append("the month has no rows")
    return problems


@dg.asset_check(
    asset=trip_features,
    partitions_def=trip_months,
    blocking=True,
    description="The table's columns, types and values match contracts/features.yaml; no post-trip column.",
)
def feature_contract(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    contract = features_contract()
    table = contract["table"]
    schema, name = table.split(".")
    actual = [
        (r["column_name"], r["data_type"])
        for r in redshift.query(
            "select column_name, data_type from information_schema.columns "
            f"where table_schema = '{schema}' and table_name = '{name}' order by ordinal_position"
        )
    ]
    problems = column_problems(actual, contract)
    conditions = value_conditions(contract)
    counts = ", ".join(f"coalesce(sum(case when {cond} then 1 else 0 end), 0) as {col}" for col, cond in conditions.items())
    row = redshift.query(f"select count(*) as rows, {counts} from {table} where trip_month = '{month}'")[0]
    rows = int(row.get("rows") or row.get("count") or 0)
    bad = {col: int(row.get(col) or 0) for col in conditions}
    problems += value_problems(bad, rows)
    return dg.AssetCheckResult(
        passed=not problems,
        severity=dg.AssetCheckSeverity.ERROR,
        description="matches the contract" if not problems else "; ".join(problems),
        metadata={
            "rows": dg.MetadataValue.int(rows),
            "columns": dg.MetadataValue.json([f"{n}:{t}" for n, t in actual]),
            "problems": dg.MetadataValue.json(problems),
        },
    )
