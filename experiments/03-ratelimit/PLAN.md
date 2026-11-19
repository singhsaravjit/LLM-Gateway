# Experiment 3 — Rate limiter accuracy across nodes

Owner: Saravjit · Issue #19 · MVP · Course concept: **consistency models**

## Question
With N gateway nodes, does a key limited to L requests/min get L in total (Redis,
strongly consistent) or up to N × L (per-node local counters)? What latency does
the shared Redis check cost?

## Setup
- Mock LLM with `RATE_LIMIT_RPS=0` (provider never limits) and low latency (`LATENCY_MS_MEAN=20`) so only our limiter matters.
- Cache **off** (`no_cache: true`) so every request reaches the limiter path the same way.
- Limit: `RATE_LIMIT_PER_MIN=600` (10 rps), `RATE_LIMIT_BURST=10`.
- Locust: one shared API key, constant offered load of **50 rps** (5× the limit), spread round-robin by the ALB, 3 minutes per run (discard first 30 s).

## Variables
| Variable | Values |
|---|---|
| Gateway nodes | 1, 2, 4, 8 |
| `RATELIMIT_BACKEND` | `local`, `redis` |

8 runs × 3 repetitions. Locally first (`docker compose --scale gateway=N` behind a local LB), then on ECS.

## Metrics
- **Admitted rate** = Σ over nodes of `requests_total{result="ok"}` / duration.
- **Over-admission %** = (admitted − allowed) / allowed × 100, where allowed = burst + rate × duration.
- **Limiter latency**: p50 / p95 / p99 of the `Allow` call (add a histogram `ratelimit_check_seconds`) and end-to-end p99.
- Redis CPU and ops/s (CloudWatch for ElastiCache).

## Expected result
| Nodes | local | redis |
|---|---|---|
| 1 | ~0% | ~0% |
| N | up to (N−1) × 100% | ~0% |

Redis adds roughly one round trip (sub-ms locally, ~0.5–1 ms in the same AZ) per request.

## Also try (if time)
- Burst-only behaviour: 200 requests at once from 8 nodes → exactly `burst` admitted (already a unit test; confirm on AWS).
- Hot key: 1 key vs 1,000 keys at the same total load — does a single Redis key become a bottleneck?

## Output
`results/*.csv` (Locust + Prometheus export), `charts/over_admission.png`, `charts/latency.png`, `RESULTS.md`.
