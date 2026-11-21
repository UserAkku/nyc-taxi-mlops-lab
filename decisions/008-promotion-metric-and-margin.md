# 008: A new version becomes `challenger` only if its holdout MAE beats the champion's by 2% and its p95 latency is within 25 ms

Stage: S2 · Date: 2026-11-21 · Author: UserAkku · Drafted by agent: yes (I chose the agent's recommendation after it laid out the options and the measured numbers)

## Choice

`eval_report` compares the new version with the current `champion` (or, before there is one, with the v1 baseline trained on the same window) on the same holdout month. The blocking check `beats_champion` passes only if both hold:

- `mae_min` (holdout MAE, minutes) is at most `champion_mae_min * (1 - 0.02)`: a 2% relative improvement;
- `p95_latency_ms` (single-row predict, in process) is at most 25 ms.

Only then does the `challenger` asset set the alias, and it re-checks the same rule itself. If either number were undecided the gate refuses everything. The pipeline never sets `champion`.

## Rejected alternative

Adding a slice guard to the MAE rule: the overall MAE must improve and no slice (airport, Manhattan, outer boroughs, night) may be more than 10% worse than the champion's.

## Why

First run, trained on 2024-01..03 and tested on 2024-04 (3.48M trips):

| | MAE (min) |
| --- | --- |
| v1 baseline | 3.956 |
| v2 LightGBM | 3.637 (8.1% better) |

v2's p95 latency was 5.4 ms on the platform host; its slice MAEs were airport 5.98, Manhattan 3.15, outer 7.42, night 2.62.

- **MAE** is what the model is trained on (`objective: l1`) and what `contracts/mlflow.yaml` records as `mae_min`, and a handful of three-hour trips don't dominate it the way they would dominate RMSE.
- **2% margin:** sampling noise on 3.5M rows is tiny (my estimate, under 0.1%, not measured); the real risk is that one test month isn't representative of the next. 2% asks for a clear improvement, so a retrain that is better by chance doesn't displace the model, while v2's 8.1% passes comfortably. 5% would hold back small but real improvements; 0% would let month-to-month variation promote models.
- **25 ms** is about five times today's 5.4 ms and far below the app's 300 ms timeout (after which it falls back to the static median). It would catch a real slowdown (the Dockerfile notes one case, a spin-wait that took p95 from 3.9 to 19.6 ms, and a 5x slower model would fail) without flagging the normal 4-5 ms variation between runs.
- I left the slice guard out because I had no baseline slice numbers to check that the first model would pass it, and a rule I can't test shouldn't block the pipeline.

Accepted trade-off: a model can improve overall MAE while getting worse on a slice (for example the airport, where v2's error is already the second highest), and this rule wouldn't notice.

## What would show this was wrong

- A promoted model that is worse for riders in a slice, such as airport trips: then the slice guard comes back, using the champion's slice MAEs that `eval_report` already computes.
- Promotions that don't hold up on the next month (the drift report rising, or the next holdout disagreeing): the margin is too small, or one holdout month isn't enough.
- A good model refused because of the latency budget on a busier host: check whether the 25 ms limit or the host is the problem before loosening it.
