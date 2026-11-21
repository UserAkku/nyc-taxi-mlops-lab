# 009: Train on the three months before the holdout; test on the latest month (2024-01..03, test 2024-04)

Stage: S2 · Date: 2026-11-21 · Author: UserAkku · Drafted by agent: yes (I chose the agent's recommendation after it laid out the options)

## Choice

`training_set` takes the latest month in `staging.trip_features` as the holdout and the three available months right before it as the training window: today 2024-01..2024-03 train, 2024-04 test. The window size (`WINDOW_MONTHS = 3`) and the holdout are configurable (`TrainConfig`), and the code refuses a holdout that has fewer than three earlier months or that appears in the window. The zone-pair distance lookup is computed from the window only.

## Rejected alternative

A random split across all four months (for example 80% of every month to train, 20% to test).

## Why

- The model is used to predict trips that haven't happened yet. Testing on the month after the training window measures exactly that. A random split puts trips from the same days, even the same hours, on both sides, so the model has effectively seen the test set's neighbours: the MAE looks better than it will be in serving (a quiet form of leakage).
- Three months is the most training data available that still leaves a later month to test on, and it is also the window the drift code (`drift.py`) assumes before there is a champion and the one the notebook's backstory uses, so the numbers are comparable across them.
- Keeping the distance lookup to the window means no trip from the test month feeds a model input.

Accepted trade-offs: one test month (April) is a small, single sample, and with only four months of 2024 any seasonal effect between winter and spring lands in the test; the MAE is an estimate, and I treat small differences between models with that in mind (the promotion margin in decision 008 exists for that reason). Using the latest month as the holdout means each new month moves the window forward.

## What would show this was wrong

- A model that looks good on April and clearly worse on the next month that arrives (the drift report's PSI rising, or `eval_report` on a later holdout disagreeing): then one month of testing isn't enough, and the holdout should span more than one month.
- Evidence that seasonality dominates: the same model's MAE differing a lot between holdout months, which would argue for a longer window that covers a full year.
