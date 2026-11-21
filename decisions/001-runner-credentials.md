# 001: Runner credentials: the app host's instance profile

Stage: S0 · Date: 2026-11-21 · Author: UserAkku · Drafted by agent: yes

## Choice

The self-hosted runner on the app host (label `lab-app`) gets AWS access only from the host's `LabInstanceProfile`, through IMDSv2. No AWS credentials are stored in GitHub for CI or CD: CI uses `GITHUB_TOKEN` to push to GHCR, CD pulls with `GITHUB_TOKEN` and runs on the host.

## Rejected alternative

Static Learner Lab session credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) in GitHub Actions secrets, copied by `scripts/gh-secrets.sh`, used by a GitHub-hosted runner or the self-hosted one.

## Why

- Session credentials die with every Learner Lab session (about four hours), so CD would break after each Start Lab until someone re-ran `gh-secrets.sh`. The instance profile's credentials rotate by themselves, so CD keeps working across session refreshes with nobody present.
- Nothing long-lived to leak from a public repo: there is no secret in GitHub to exfiltrate through a malicious workflow change or a log line.
- A GitHub-hosted runner couldn't reach the host's private network anyway; the deploy has to run on the host, and the host already has a role.

Accepted trade-off: every job that runs on this runner gets `LabRole`'s permissions. Mitigated by: no self-hosted job triggers on `pull_request`/`pull_request_target` (`make check` enforces it), fork-PR workflows need approval for all external contributors, and only pushes to `main` reach the runner.

## What would show this was wrong

- A workflow that must call AWS from a GitHub-hosted runner (the S10 destroy workflow is the known case: that one does need `gh-secrets.sh`).
- Evidence that an untrusted change ran on the runner (an unexpected job in the runner's history, or AWS calls from the instance we didn't make), which would mean the runner's blast radius (all of `LabRole`) is too big.
