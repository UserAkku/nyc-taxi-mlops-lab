# 007: The notebook explores; everything that registers, gates or aliases a model is in the pipeline

Stage: S2 · Date: 2026-11-21 · Author: UserAkku · Drafted by agent: yes (I chose the agent's recommendation after it laid out the options)

## Choice

The SageMaker notebook (`notebooks/explore.ipynb`) is for looking at the data (plots, distributions, per-zone statistics) and trying quick models on small samples. Everything that produces something the system relies on lives in `pipelines/`, in version control and covered by tests: the feature SQL (`trip_features`), the `feature_contract` check, training on the full window, evaluation, the promotion gate, MLflow registration and the `challenger` alias. Nothing is registered or aliased from the notebook, and its runs go to their own experiment (`trip-duration-notebook`).

## Rejected alternative

Letting the notebook also tune hyperparameters on bigger samples and copying the winning values into the pipeline by hand.

## Why

- A model that gets served has to be reproducible: the same window, the same code, the same gate, every time, with the git commit recorded on the version (`git_sha`). A notebook cell run in some order on some sample isn't that.
- The notebook's results are easy to get wrong silently: a sample isn't the holdout, cells run out of order, and a hand-copied hyperparameter can differ from the one that produced the number. Keeping the notebook off the registry removes that whole class of mistake, and nobody can put a model behind the `challenger` alias without passing the blocking gate.
- It matches the repo's rules (`AGENTS.md`: never register a model from a notebook) and keeps the notebook cheap: it can stay on an `ml.t3.medium` because it never trains on the full data.

Accepted trade-off: trying an idea takes a pipeline run (minutes) instead of a notebook cell, and tuning has to be done in the notebook on samples, with the result confirmed by the pipeline's holdout, not trusted.

## What would show this was wrong

- Improvement ideas piling up in the notebook that nobody can evaluate on the full holdout without a pipeline change: then the notebook should get a way to run the pipeline's training function (same code, bigger data), not its own copy.
- A number quoted from the notebook that the pipeline's `eval_report` doesn't reproduce.
