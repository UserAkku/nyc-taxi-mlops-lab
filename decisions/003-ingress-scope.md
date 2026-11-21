# 003: Ingress scope: port 80 on the app host open to everyone (0.0.0.0/0)

Stage: S0 · Date: 2026-11-21 · Author: UserAkku · Drafted by agent: yes

## Choice

`ingress_cidrs = ["0.0.0.0/0"]`: anyone on the internet may reach Traefik on port 80 of the app host. Only port 80 is open; every other port on every host is reachable only from named security groups (MLflow, LLM, Postgres), and there is no port 22.

## Rejected alternative

Only my own IP and the grader's IP, each as a `/32`.

## Why

- The app is a rider-facing API: a real ride app is reached from any phone on any network, so a public endpoint is the honest shape of the system.
- An allow-list of `/32`s breaks whenever my IP changes (switching Wi-Fi, a mobile hotspot, the ISP rotating my address), and each fix needs a `terraform apply` with fresh session credentials. A missing or stale grader IP would also fail S0's done checks.
- The blast radius is bounded: Traefik only forwards to the app; the app holds no secrets in S0 and only serves `/healthz`, `/version`, `/predict` (a static number) and `/metrics`.

Accepted trade-offs, which I have to revisit:
- Scanners and bots will hit it within minutes; that traffic shows up in logs and, from S5, in request metrics and SLOs.
- There is no authentication or rate limit. From S6, `POST /assist` calls the LLM host, so anyone could burn its CPU; `/metrics` is public too.

## What would show this was wrong

- Unknown traffic distorting S5's latency/error SLOs or alerts.
- `/assist` (S6) being reachable by strangers, or LLM host CPU pinned by requests that aren't mine or the grader's: at that point either narrow `ingress_cidrs`, or add auth or a rate limit in Traefik before going further.
