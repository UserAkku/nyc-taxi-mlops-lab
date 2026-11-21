"""S1: the schema_contract check on trip_files.

Compare the landed file's Parquet footer (the DataBucket resource reads it) with
contracts/trips.schema.yaml, before raw_trips copies anything. contracts/platform.yaml says this check
is blocking. Patterns: pipelines/reference/patterns.py.

What counts as a mismatch follows the contract's own header: the same number of columns, in the same
order (Parquet COPY maps by position), the same names compared case-insensitively (TLC renamed
airport_fee to Airport_fee and positional COPY never sees names), and the same Parquet storage types.
Anything else, including a new column appended at the end, is a mismatch: the contract is evolved on
purpose (and recorded), never absorbed by a check that quietly lets it through.
"""

import dagster as dg

from ..contracts import Column, trips_schema
from ..partitions import trip_months
from ..resources import DataBucket
from .trip_files import trip_files


def schema_problems(expected: list[Column], found: list[Column]) -> list[str]:
    """Every way `found` differs from `expected`, as short sentences; empty when they match."""
    problems = []
    if len(found) != len(expected):
        problems.append(f"{len(found)} columns, the contract has {len(expected)}")
    for i, (want, got) in enumerate(zip(expected, found, strict=False), start=1):
        if want.name.lower() != got.name.lower():
            problems.append(f"column {i} is named {got.name}, the contract says {want.name}")
        if want.type != got.type:
            problems.append(f"column {i} ({got.name}) is {got.type}, the contract says {want.type}")
    extra = [c.name for c in found[len(expected) :]]
    if extra:
        problems.append(f"extra column(s) after the contract's last one: {', '.join(extra)}")
    missing = [c.name for c in expected[len(found) :]]
    if missing:
        problems.append(f"missing column(s) from the contract: {', '.join(missing)}")
    return problems


@dg.asset_check(
    asset=trip_files,
    partitions_def=trip_months,
    blocking=True,
    description="The file's Parquet footer matches contracts/trips.schema.yaml (count, order, names, types).",
)
def schema_contract(context: dg.AssetCheckExecutionContext, data_bucket: DataBucket) -> dg.AssetCheckResult:
    footer = data_bucket.footer(context.partition_key)
    problems = schema_problems(trips_schema(), footer.columns)
    return dg.AssetCheckResult(
        passed=not problems,
        severity=dg.AssetCheckSeverity.ERROR,
        description="matches the contract" if not problems else "; ".join(problems),
        metadata={
            "columns_found": dg.MetadataValue.int(len(footer.columns)),
            "problems": dg.MetadataValue.json(problems),
        },
    )
