# 004: Load trips with COPY into Redshift, not Spectrum

Stage: S1 · Date: 2026-11-21 · Author: UserAkku · Drafted by agent: yes

## Choice

Each month's Parquet file is loaded into the warehouse (`ingest.raw_trips`) with `COPY ... iam_role default format as parquet`, one month per Dagster partition. Everything downstream (`stg_trips`, `zone_hour_stats`) reads Redshift's own tables.

## Rejected alternative

Redshift Spectrum: leave the files in the instructor's bucket, expose them as an external table, and query them in place.

## Why

- Queries on data stored in Redshift are faster than scanning Parquet in S3 on every read, and the mart, the S2 training-set UNLOAD and the checks all read these rows repeatedly.
- The given pipeline is shaped for it: `stg_trips` selects from a real table with a `source_file` column, and the reload pattern (delete the month's rows, insert them again, in one transaction) only makes sense for a table I own. With Spectrum, `raw_trips` and part of `stg_trips` would have to change.
- A month's load is a single, bounded step I can measure and make idempotent, instead of a dependency on an external table's catalog and permissions that the rest of the stage doesn't need.

Accepted trade-offs: each month is stored twice (S3 and Redshift), a new file needs a load run before it's queryable (the sensor does that), and a file replaced upstream has to be reloaded (the sensor requests that when the ETag changes).

## What would show this was wrong

- Redshift storage or load time becoming a real problem (many more months, or much larger files).
- Needing to query very recent files before the load finishes, or files nobody loads (ad-hoc exploration), where Spectrum's no-load behaviour would pay off.
