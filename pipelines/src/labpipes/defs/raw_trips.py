"""S1: the raw_trips asset.

One partition per month (partitions.trip_months), depending on trip_files. COPY the month's file into
Redshift with the LabRole attached to the cluster (`iam_role default`), so that re-running a month never
duplicates its rows. The landing columns come from contracts/trips.schema.yaml
(contracts.redshift_columns). Decision points: COPY vs Spectrum; dist and sort keys.

Decisions (decisions/004, 005): COPY into the warehouse, and ingest.raw_trips is DISTSTYLE EVEN with
SORTKEY (source_file).

Why a temp table: the file's Parquet columns are copied by position, and raw_trips has one more column,
source_file, which says which file a row came from (stg_trips and the reload below both use it). So the
month is copied into a temp table with exactly the file's columns, then inserted with its source_file.

Why it's safe to run twice: everything below is ONE transaction (RedshiftData.execute with several
statements). It deletes the month's rows (by source_file) and inserts them again, so a second run leaves
the same rows, and a COPY that fails halfway rolls the delete back too.
"""

import dagster as dg

from ..contracts import redshift_columns, trips_schema
from ..partitions import source_file, trip_months
from ..resources import DataBucket, RedshiftData
from .trip_files import trip_files


def raw_trips_statements(month: str, uri: str) -> list[str]:
    """The reload of one month. `month` is validated by source_file(); `uri` comes from DataBucket.trip_uri."""
    file = source_file(month)
    cols = redshift_columns(trips_schema())
    names = ", ".join(c.name.lower() for c in trips_schema())
    return [
        "create schema if not exists ingest",
        f"create table if not exists ingest.raw_trips (source_file varchar(64) not null, {cols}) "
        "diststyle even sortkey (source_file)",
        f"create temp table landing ({cols})",
        f"copy landing from '{uri}' iam_role default format as parquet",
        f"delete from ingest.raw_trips where source_file = '{file}'",
        f"insert into ingest.raw_trips select '{file}', {names} from landing",
    ]


@dg.asset(
    partitions_def=trip_months,
    deps=[trip_files],
    group_name="ingest",
    kinds={"redshift"},
    pool="redshift",
)
def raw_trips(
    context: dg.AssetExecutionContext, redshift: RedshiftData, data_bucket: DataBucket
) -> dg.MaterializeResult:
    month = context.partition_key
    redshift.execute(*raw_trips_statements(month, data_bucket.trip_uri(month)))
    rows = redshift.scalar(f"select count(*) from ingest.raw_trips where source_file = '{source_file(month)}'")
    return dg.MaterializeResult(
        metadata={"rows": dg.MetadataValue.int(int(rows)), "source": data_bucket.trip_uri(month)}
    )
