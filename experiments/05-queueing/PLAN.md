# Experiment 5 — Queueing under provider rate limits

Owner: Saravjit · Issue #19 · MVP · Course concept: **asynchronous messaging and backpressure**

## Question
When offered load exceeds the provider's limit, how much does the overflow queue
improve success rate, and what does it cost in latency? Where does backpressure
kick in?

## Setup
- Mock LLM: `RATE_LIMIT_RPS=50`, `LATENCY_MS_MEAN=800`, `LATENCY_MS_JITTER=300`.
- Gateway: 2 nodes, cache **off**, per-key limiter high enough not to interfere (many keys).
- Provider bucket `PROVIDER_RPS_CHEAP=45` (just under the mock's 50).
- Locust: constant offered load, 4 minutes per run (discard first 30 s), client timeout 60 s.
- Baseline: Locust → mock directly (no gateway).

## Variables
| Variable | Values |
|---|---|
| Offered load (× provider limit) | 0.5×, 1×, 1.5×, 2×, 3× (25 → 150 rps) |
| `QUEUE_ENABLED` | off, on |
| `QUEUE_MAX` | 100, 1000, 10000 (queue on only) |
| Load shape (extra) | 30 s burst at 3× then back to 0.5× |

## Metrics
- **Success rate** (2xx / total) and **client-visible 429s**
- p50 / p95 / p99 latency (Locust)
- **Dropped**: `queue_rejected_total{reason}` — full / timeout / expired
- **Queue depth** and **wait time** over time (Grafana)
- Upstream 429s actually received from the mock (should be ≈0 with the provider bucket)

## Expected result
- Queue off: success ≈ min(1, 50 / offered); excess becomes 429s.
- Queue on, sustained overload: the queue fills, then backpressure — throughput stays at ~45 rps, extra load becomes `queue_full` or `queue_timeout`. **A queue cannot fix sustained overload; it only moves where the requests fail.**
- Queue on, short burst: near-100% success, p99 grows by roughly (burst size / 45 rps).
- Bigger `QUEUE_MAX`: fewer `queue_full`, more `queue_timeout` and higher p99 → pick a size from wait-time budget, not "as large as possible".

## Output
`results/*.csv`, `charts/success_rate.png`, `charts/p99.png`, `charts/queue_depth_burst.png`, `RESULTS.md`.
