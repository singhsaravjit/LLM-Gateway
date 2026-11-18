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
