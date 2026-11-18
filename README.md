# LLM Request Gateway

**CS 6650 Final Project** — a horizontally scalable gateway that sits between applications and LLM providers and adds **shared caching, distributed rate limiting, per-team budgets, request queueing, and cheap/strong model routing**.

Team: Dinesh Bachchani · Kanad Motiwale · Preshit Ravindra Pimple · Saravjit Singh

```
client ──► ALB ──► gateway nodes (stateless, ECS autoscaled)
                    │ 1. rate limit + budget ─┐
                    │ 2. cache lookup ────────┼──► Redis (cache, counters, budgets)
                    │ 3. router (cheap/strong)│
                    │ 4. provider client ─────┴──► queue ──► workers ──► LLM provider / mock
                    └──► /metrics ──► Prometheus ──► Grafana
```

## Quick start

**No Docker** (two terminals):
```bash
make run-mock      # mock LLM on :9000
make run-gateway   # gateway on :8080
make smoke         # first call X-Cache: MISS, second: HIT
```

**Full stack** (gateway, mock, Redis, Prometheus, Grafana):
```bash
make up            # Grafana :3000 (admin/admin), Prometheus :9090
make load          # Locust UI on :8089
make down
```

## Repo layout

| Path | What | Owner |
|---|---|---|
| `cmd/gateway` | HTTP gateway, request pipeline | Preshit |
| `cmd/mockllm` | Mock provider (latency, 429s, errors, tokens) | Kanad |
| `internal/api` | Shared request/response types — **the contract** | all |
| `internal/router`, `internal/provider` | Model routing, provider client, retries, circuit breaker | Preshit |
| `internal/cache` | Shared cache, coalescing, semantic cache (stretch) | Dinesh |
| `internal/ratelimit`, `internal/budget`, `internal/queue` | Token bucket, spend counters, overflow queue | Saravjit |
| `internal/metrics`, `deploy/`, `loadtest/` | Metrics, Terraform, Prometheus/Grafana, Locust | Kanad |
| `docs/api.md` | API, error codes, Redis key formats, log schema | all |
| `experiments/` | Experiment configs and results (7 experiments) | each owner |

## Current state

The skeleton runs end to end with **in-memory** components so everyone can develop independently.
Each owner replaces their in-memory implementation with the Redis/distributed version behind the same interface.

## Plan (3.5 weeks, done by Sat Dec 12)

Tracked as [GitHub milestones](https://github.com/singhsaravjit/LLM-Gateway/milestones); every member owns 6 issues.

| Milestone | Dates | Goal |
|---|---|---|
| Week 1 · Foundations | Nov 18 – 24 | Contract in `docs/api.md` agreed, local stack running, first Redis-backed cache and limiter, mock calibrated |
| Week 2 · Distributed features + AWS | Nov 25 – Dec 1 | Coalescing, queue, budgets, retries/fallback, Terraform up; **all components integrated in Compose by Sun Nov 29** |
| Week 3 · Experiments | Dec 2 – 8 | First full AWS deploy (Dec 2), then all MVP experiments, stretch ones if time allows |
| Final · Report + demo | Dec 9 – 12 | Report draft Dec 10, demo rehearsal Dec 11, submit Dec 12 |

| Member | Week 1 | Week 2 | Week 3 | Final |
|---|---|---|---|---|
| Preshit | Gateway hardening, request middleware | Retries / circuit breaker / fallback, router + real provider | Exp. 6 | Report + demo section |
| Dinesh | Redis cache, shared Redis client | Coalescing, cache metrics + cost saved | Exp. 1, 2 | Report + demo section |
| Saravjit | Redis rate limiter, queue + budget design | Overflow queue, Redis budgets | Exp. 3, 5 (4) | Report + demo section |
| Kanad | Compose stack + dashboard, mock calibration | Terraform, image publishing + autoscaling | Exp. 7 | Report + demo section |

Thanksgiving (Nov 26) falls in Week 2, the heaviest week — get ahead in Week 1. If time runs short, drop stretch items first.

**MVP:** exact-match cache, Redis rate limiting per key, rule-based router, mock LLM, ECS + Terraform, dashboards, Experiments 1, 3, 5, 7.
**Stretch:** semantic cache, monthly budgets with alerts, priority queues, streaming, Experiments 2, 4, 6.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the branch/PR workflow.
