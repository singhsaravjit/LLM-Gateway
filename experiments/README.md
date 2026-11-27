# Experiments

Each experiment gets its own folder (`experiments/0N-name/`) with: the config
used, the Locust command, raw results (CSV), and a short `RESULTS.md`.
Change **one setting at a time** and always compare against the baseline
(clients calling the mock provider directly).

| #  | Experiment                 | Owner    | Vary                                            | Measure                                   | Scope   |
|----|----------------------------|----------|-------------------------------------------------|-------------------------------------------|---------|
| 1  | Caching                    | Dinesh   | cache on/off, TTL, repeat rate                  | hit rate, latency, cost saved             | MVP     |
| 2  | Request coalescing         | Dinesh   | burst of identical prompts, coalescing on/off   | calls reaching provider, latency          | Stretch |
| 3  | Rate limiter accuracy      | Saravjit | 1/2/4/8 nodes; local vs Redis counters          | over-admission %, added latency           | MVP     |
| 4  | Redis failure              | Saravjit | Redis slow/down; fail open vs closed            | errors, requests let through, recovery    | Stretch |
| 5  | Queueing under 429s        | Saravjit | queue on/off, size, load above provider limit   | success rate, p99, dropped requests       | MVP     |
| 6  | Routing and failover       | Preshit  | cheap-model share; errors injected into strong  | cost/1k req, failover time, error rate    | Stretch |
| 7  | Horizontal scaling         | Kanad    | load level, autoscaling thresholds              | max throughput, scale-out/in time         | MVP     |

## Tooling (Exp 3–5)

| Path | What |
|---|---|
| `tools/loadgen` | Open-loop load generator: fixed arrival rate, round-robin over nodes, per-request CSV. Used instead of Locust where the offered load must stay constant even when the system slows down (Locust is closed-loop). |
| `tools/common.sh` | Starts the mock and N real gateway processes against a Redis you provide. |
| `tools/charts.py` | Report charts from the run CSVs (`pip install matplotlib`). |
| `0N-*/run.sh` | One script per experiment; writes `results/summary.csv`, raw per-request CSVs and logs. |
| `0N-*/RESULTS.md` | Setup as run, tables, charts, findings, caveats, what is left. |

## Status

| # | Status |
|---|---|
| 1 | Preliminary results, local (1 run per setting) — [RESULTS](01-caching/RESULTS.md) |
| 2 | Preliminary results, local (1 burst per setting) — [RESULTS](02-coalescing/RESULTS.md) |
| 3 | Preliminary results, local (3 repeats) — [RESULTS](03-ratelimit/RESULTS.md) |
| 4 | Preliminary results, local (1 run per case) — [RESULTS](04-redis-failure/RESULTS.md) |
| 5 | Preliminary results, local (2 repeats steady, 1 burst) — [RESULTS](05-queueing/RESULTS.md) |

All of them are to be repeated on ECS once the AWS stack (#21, #22) is up.
