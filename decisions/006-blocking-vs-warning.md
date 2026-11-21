# 006: Which data-quality checks block, and their thresholds

Stage: S1 · Date: 2026-11-21 · Author: UserAkku · Drafted by agent: yes (I asked the agent to choose; it chose and gave the reasons below, which I have to be able to defend)

## Choice

Eleven checks on `stg_trips`, one partition per month. **Eight block** (a failure stops `zone_hour_stats` from being rebuilt in that run) and three only warn.

| Check | Rule | Limit | Blocks |
| --- | --- | --- | --- |
| `rows_complete` | rows in `stg_trips` equal the file's Parquet footer row count | exact | yes |
| `rows_sane` | the month's row count | 1.48M to 7.2M | yes |
| `no_double_load` | exact duplicate trips | more than 0.1% | yes |
| `critical_nulls` | null pickup/dropoff time or zone | more than 0 rows | yes |
| `dates_in_month` | pickup outside the partition's month | more than 0.1% | yes |
| `bad_duration` | duration <= 0 | more than 0.35% | yes |
| `very_long_trips` | duration over 3 hours | more than 0.7% | yes |
| `unknown_zones` | a zone outside 1..263 | more than 11% | yes |
| `null_passengers` | no passenger count | more than 25% | warn |
| `negative_fare` | fare below 0 | more than 3.5% | warn |
| `zero_distance` | distance <= 0 | more than 5% | warn |

`schema_contract` on `trip_files` also blocks (the platform contract requires it).

## Rejected alternative

Making every check blocking (including passengers, fare and distance), with round-number limits (1% for duration, 5% for zones).

## Why

Measured in Redshift on 2024-01..04 (13.07M trips), worst real month per check:

| Check | Worst real month | Limit | Limit / worst |
| --- | --- | --- | --- |
| `bad_duration` | 0.031% | 0.35% | about 11x |
| `very_long_trips` | 0.067% | 0.7% | about 10x |
| `unknown_zones` | 1.06% | 11% | about 10x |
| `dates_in_month` | 0.0006% | 0.1% | about 170x |
| `null_passengers` | 11.9% (4.7% in January) | 25% | about 2x |
| `negative_fare` | 1.64% | 3.5% | about 2x |
| `zero_distance` | 2.43% | 5% | about 2x |

- The mart's SQL already drops bad rows (duration <= 0 or over 3 h, zones outside 1..263), so a handful of bad trips never reaches it. The checks are there to stop a bad *month*: loaded twice, timestamps shifted, a column shifted, rows missing. A corrupt month looks 10-50% wrong, not 0.4%.
- Blocking is reserved for the columns the mart and the S2 model use (times, duration, zones) and for load integrity. Passengers, fare and distance aren't used by either, so they warn: blocking on them would only stop good data.
- A real, normal shift shows why a tight limit hurts: null passengers rose from 4.7% to 11.9% in four months. With a 3x-worst limit (or a 5% limit) March and April would have been blocked although they are fine. About 10x worst for the rates that are tiny in real data leaves room for ordinary variation and still catches a corrupt month. Round numbers (1% for duration) sit about 30x above the real rate, which lets a quite broken month through.
- `critical_nulls` has no tolerance because every real month has exactly zero, and a null time or zone means the row can't be placed in the mart.
- Tested against the four real months (none fails anything, not even a warning) and against corrupt months (loaded twice, partial copy, half a file, shifted timestamps, negative durations, shifted zone column, a single null).

Accepted trade-offs: limits come from four months of 2024 only; a new, legitimate change (a new year's data) could trip a check and need a deliberate re-decision. The 11% zone limit is wide on purpose: a shifted column can still look partly valid.

## What would show this was wrong

- A real month being blocked (a false alarm): then the limit is too tight, or the data really changed and needs a contract decision.
- A corrupt month reaching `zone_hour_stats` (gameday): a check that should have caught it is missing or too loose. The warning checks are the first suspects (a corrupt month that only trips `null_passengers`, say).
- `null_passengers` keeps climbing toward 25%: it is a warning for a reason; if it passes the limit, decide whether passengers matter for the model.
