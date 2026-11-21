# 005: ingest.raw_trips is DISTSTYLE EVEN with SORTKEY (source_file)

Stage: S1 · Date: 2026-11-21 · Author: UserAkku · Drafted by agent: yes

## Choice

`ingest.raw_trips` is created with `diststyle even sortkey (source_file)`. `staging.stg_trips` keeps the sort key it was given (`sortkey (pickup_at)`).

## Rejected alternative

Distributing on the pickup zone (`distkey (pulocationid)`), so one zone's trips live on one slice.

## Why

- **Sort key:** every month's reload and every read of `raw_trips` filters on `source_file` (`delete ... where source_file = 'yellow_tripdata_2024-03.parquet'`, and `stg_trips` selects `where source_file = ...`). With that sort key a month's rows sit together and Redshift skips the blocks of other months.
- **EVEN, not KEY:** the cluster has one `ra3.large` node (2 slices), so co-locating a zone's rows saves very little data movement, while taxi trips are heavily skewed toward a few zones (JFK, Midtown): a zone key could leave one slice with far more rows. EVEN keeps both slices equally loaded and needs no assumption about the data.
- I did not choose `AUTO` because I want the layout to be a choice I can explain and measure, not a default.

I did not measure skew or query times for this record: it rests on the cluster's size and the access pattern of the SQL in this repo.

## What would show this was wrong

- The zone-wise group-by in `zone_hour_stats` getting slow as months are added, or a second node being added (more slices make co-location matter more): then compare `KEY(pulocationid)` on measured timings (`svv_table_info`: `skew_rows`; `stl_query` durations).
- A reload or read of one month that is not fast (the `source_file` sort key not helping, e.g. unsorted rows growing: `svv_table_info.unsorted`).
