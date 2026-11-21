"""S1: the S3 sensor (and the job it starts).

Watch trips/ in the data bucket (DataBucket.trip_files). When a month's file appears or changes, add
the month to the trip_months partitions and request a run that goes from trip_files to zone_hour_stats.
Patterns: pipelines/reference/patterns.py.

The cursor remembers each month's ETag (S3's fingerprint of the file's content). A month not in the cursor
is new; a month whose ETag moved was replaced upstream. Either way one run is requested, and the run_key
(month + ETag) makes asking twice for the same file a no-op.

The job also reloads taxi_zones: it's tiny, and the mart's zone names come from it, so a run never builds
the mart on a lookup table that was never loaded.
"""

import json

import dagster as dg

from ..partitions import trip_months
from ..resources import DataBucket

ingest_month = dg.define_asset_job(
    "ingest_month",
    selection=dg.AssetSelection.assets("taxi_zones", "trip_files", "raw_trips", "stg_trips", "zone_hour_stats"),
    description="One month, trip file to mart. A blocking check that fails upstream keeps the rest from running.",
)


@dg.sensor(job=ingest_month, minimum_interval_seconds=60, default_status=dg.DefaultSensorStatus.RUNNING)
def trip_files_sensor(context: dg.SensorEvaluationContext, data_bucket: DataBucket):
    seen: dict[str, str] = json.loads(context.cursor or "{}")
    found = {f.month: f.etag for f in data_bucket.trip_files()}
    changed = sorted(m for m, etag in found.items() if seen.get(m) != etag)
    if not changed:
        return dg.SkipReason(f"nothing new among {len(found)} file(s)")
    new_partitions = [m for m in changed if not context.instance.has_dynamic_partition(trip_months.name, m)]
    return dg.SensorResult(
        run_requests=[dg.RunRequest(partition_key=m, run_key=f"{m}:{found[m]}") for m in changed],
        dynamic_partitions_requests=[trip_months.build_add_request(new_partitions)] if new_partitions else [],
        cursor=json.dumps({**seen, **{m: found[m] for m in changed}}),
    )
