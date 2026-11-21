"""S1: data-quality checks on stg_trips (nulls, ranges, duplicates, row counts).

Decision points: which checks block downstream and which only warn, and every threshold. Base them on
what real months look like (scripts/verify/results/tlc-stats-2024-01-04.md is the instructor's
baseline; measure your own in Redshift) and on what gameday's corrupt month would look like.

Decision record 006 has the reasoning. In short: the mart's SQL already drops bad rows (duration <= 0 or
> 3 h, zones outside 1..263), so a few bad trips don't hurt it. These checks exist to stop a bad *month*
(loaded twice, timestamps shifted, rows missing) from reaching it. Blocking checks guard the columns the
mart and the S2 model use; checks on columns they don't use (passengers, fare, distance) only warn.

Measured on 2024-01..04 in Redshift (13.07M trips): worst real month per check, and the limit chosen.

    check            worst real month   limit          blocks
    rows_complete    equal              equal          yes   rows in stg_trips == rows in the file's footer
    rows_sane        2.96M-3.58M        1.48M-7.2M     yes   half the smallest month .. twice the largest
    no_double_load   0.00003%           > 0.1%         yes   exact duplicate rows
    critical_nulls   0                  > 0 rows       yes   null pickup/dropoff time or zone
    dates_in_month   0.0006%            > 0.1%         yes   pickup outside the partition's month
    bad_duration     0.031%             > 0.35%        yes   duration <= 0           (10x worst, rounded up)
    very_long_trips  0.067%             > 0.7%         yes   duration > 3 hours      (10x worst, rounded up)
    unknown_zones    1.06%              > 11%          yes   zone outside 1..263     (10x worst, rounded up)
    null_passengers  11.9% (rising)     > 25%          warn  (2x worst)
    negative_fare    1.64%              > 3.5%         warn  (2x worst, rounded)
    zero_distance    2.43%              > 5%           warn  (2x worst, rounded)

Each check is its own @asset_check (Dagster 1.13's multi_asset_check can't be partitioned) and asks
Redshift only what it needs. A blocking check that fails keeps everything downstream (zone_hour_stats)
from running in the same run.
"""

from dataclasses import dataclass

import dagster as dg

from ..partitions import check_month, trip_months
from ..resources import DataBucket, RedshiftData
from .staging import stg_trips

ROWS_MIN, ROWS_MAX = 1_480_000, 7_200_000

# name -> (largest allowed share of the month's rows, blocking, SQL condition that counts a bad row, description).
# The SQL is formatted with a validated month and runs against staging.stg_trips.
RATE_CHECKS = {
    "no_double_load": (0.001, True, None, "exact duplicate rows"),  # counted by DUPLICATES_SQL
    "critical_nulls": (0.0, True, "pickup_at is null or dropoff_at is null or pickup_zone_id is null or dropoff_zone_id is null",
                       "trips with a null pickup/dropoff time or zone"),
    "dates_in_month": (0.001, True, "to_char(pickup_at, 'YYYY-MM') <> '{month}'", "pickups outside the partition's month"),
    "bad_duration": (0.0035, True, "duration_min <= 0", "trips with duration <= 0"),
    "very_long_trips": (0.007, True, "duration_min > 180", "trips longer than 3 hours"),
    "unknown_zones": (0.11, True, "pickup_zone_id not between 1 and 263 or dropoff_zone_id not between 1 and 263",
                      "trips with a zone outside 1..263"),
    "null_passengers": (0.25, False, "passenger_count is null", "trips with no passenger count"),
    "negative_fare": (0.035, False, "fare_amount < 0", "trips with a negative fare"),
    "zero_distance": (0.05, False, "trip_distance <= 0", "trips with distance <= 0"),
}
BLOCKING = {"rows_complete", "rows_sane"} | {n for n, (_, blocking, _, _) in RATE_CHECKS.items() if blocking}
NAMES = ["rows_complete", "rows_sane", *RATE_CHECKS]

# Rows beyond the first of each group of identical trips.
DUPLICATES_SQL = """
select coalesce(sum(n - 1), 0) from (
  select count(*) as n from staging.stg_trips where trip_month = '{month}'
  group by vendor_id, pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id, passenger_count,
           trip_distance, ratecode_id, payment_type, fare_amount, total_amount
  having count(*) > 1)
"""


@dataclass(frozen=True)
class Outcome:
    passed: bool
    value: float
    limit: str
    description: str


def judge_rows_complete(rows: int, file_rows: int) -> Outcome:
    return Outcome(rows == file_rows, rows, f"== {file_rows} (the file's rows)", f"{rows} rows in stg_trips, {file_rows} in the file")


def judge_rows_sane(rows: int) -> Outcome:
    return Outcome(ROWS_MIN <= rows <= ROWS_MAX, rows, f"{ROWS_MIN}..{ROWS_MAX}", f"{rows} rows, a real month has 2.96M-3.58M")


def judge_rate(name: str, bad_rows: int, rows: int) -> Outcome:
    """One rate check: bad rows as a share of the month. An empty month fails (there's nothing to trust)."""
    limit, _, _, what = RATE_CHECKS[name]
    rate = bad_rows / rows if rows else 1.0
    return Outcome(rate <= limit, rate, f"<= {limit:.4%}", f"{bad_rows} {what} ({rate:.4%})")


def _result(o: Outcome, blocking: bool) -> dg.AssetCheckResult:
    return dg.AssetCheckResult(
        passed=o.passed,
        severity=dg.AssetCheckSeverity.ERROR if blocking else dg.AssetCheckSeverity.WARN,
        description=o.description,
        metadata={"value": dg.MetadataValue.float(float(o.value)), "limit": o.limit},
    )


def _rate(context: dg.AssetCheckExecutionContext, redshift: RedshiftData, name: str) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    _, blocking, condition, _ = RATE_CHECKS[name]
    rows = int(redshift.scalar(f"select count(*) from staging.stg_trips where trip_month = '{month}'") or 0)
    if condition is None:
        bad = int(redshift.scalar(DUPLICATES_SQL.format(month=month)) or 0)
    else:
        bad = int(
            redshift.scalar(
                f"select coalesce(sum(case when {condition.format(month=month)} then 1 else 0 end), 0) "
                f"from staging.stg_trips where trip_month = '{month}'"
            )
            or 0
        )
    return _result(judge_rate(name, bad, rows), blocking)


def _check(name: str):
    return dg.asset_check(
        asset=stg_trips, name=name, partitions_def=trip_months, blocking=name in BLOCKING,
        description="Decision 006: limits and the data behind them are in this module's docstring.",
    )


@_check("rows_complete")
def rows_complete(context: dg.AssetCheckExecutionContext, redshift: RedshiftData, data_bucket: DataBucket):
    month = check_month(context.partition_key)
    rows = int(redshift.scalar(f"select count(*) from staging.stg_trips where trip_month = '{month}'") or 0)
    return _result(judge_rows_complete(rows, data_bucket.footer(month).num_rows), blocking=True)


@_check("rows_sane")
def rows_sane(context: dg.AssetCheckExecutionContext, redshift: RedshiftData):
    month = check_month(context.partition_key)
    rows = int(redshift.scalar(f"select count(*) from staging.stg_trips where trip_month = '{month}'") or 0)
    return _result(judge_rows_sane(rows), blocking=True)


@_check("no_double_load")
def no_double_load(context: dg.AssetCheckExecutionContext, redshift: RedshiftData):
    return _rate(context, redshift, "no_double_load")


@_check("critical_nulls")
def critical_nulls(context: dg.AssetCheckExecutionContext, redshift: RedshiftData):
    return _rate(context, redshift, "critical_nulls")


@_check("dates_in_month")
def dates_in_month(context: dg.AssetCheckExecutionContext, redshift: RedshiftData):
    return _rate(context, redshift, "dates_in_month")


@_check("bad_duration")
def bad_duration(context: dg.AssetCheckExecutionContext, redshift: RedshiftData):
    return _rate(context, redshift, "bad_duration")


@_check("very_long_trips")
def very_long_trips(context: dg.AssetCheckExecutionContext, redshift: RedshiftData):
    return _rate(context, redshift, "very_long_trips")


@_check("unknown_zones")
def unknown_zones(context: dg.AssetCheckExecutionContext, redshift: RedshiftData):
    return _rate(context, redshift, "unknown_zones")


@_check("null_passengers")
def null_passengers(context: dg.AssetCheckExecutionContext, redshift: RedshiftData):
    return _rate(context, redshift, "null_passengers")


@_check("negative_fare")
def negative_fare(context: dg.AssetCheckExecutionContext, redshift: RedshiftData):
    return _rate(context, redshift, "negative_fare")


@_check("zero_distance")
def zero_distance(context: dg.AssetCheckExecutionContext, redshift: RedshiftData):
    return _rate(context, redshift, "zero_distance")
