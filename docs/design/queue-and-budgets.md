# Design: overflow queue and team budgets

Owner: Saravjit · Issue #6 · Status: **proposed — team to confirm in the PR**

## 1. Overflow queue

### What it has to do
When the provider is at its rate limit, requests should **wait instead of failing**
(proposal: "requests that would hit a provider limit wait in a queue"). Workers
drain the queue at a safe rate; when the queue is full we reject with
`503 queue_full` (backpressure).

The constraint that drives the design: **the client is waiting on an open HTTP
connection to one specific gateway node.** Whatever executes the job, the result
has to get back to that node within the client's timeout.

### Options

| | A. In-process queue + shared provider bucket | B. Redis Streams | C. Amazon SQS |
|---|---|---|---|
| Where jobs wait | Go channel on the receiving node | `queue:provider` stream in Redis | SQS queue |
| Who runs them | Workers on the same node | Workers on **any** node (consumer group) | Workers on any node |
| Global provider rate | Shared token bucket `rl:provider:<model>` | Same shared bucket | Same shared bucket |
| Result back to origin node | Trivial (same process) | Per-node reply stream | Needs a second channel (Redis/SNS) anyway |
| Queue size / depth | Per node | **Global** (`XLEN`) | Global (approximate) |
| Added latency per job | ~0 | ~1 ms (2–3 Redis ops) | ~10–50 ms per send/receive, long polling |
| New infrastructure | None | None (Redis already there) | SQS + IAM in Terraform, LocalStack for local dev |
| Survives node crash | No | Jobs yes (`XAUTOCLAIM`) | Jobs yes (visibility timeout) |
| Shows "async messaging" (course concept) | Weakly | Yes | Yes |
| Build effort | ~1 day | ~2–3 days | ~3–4 days + Kanad's Terraform |

Durability matters less than it looks: if the origin node dies, its client's
connection dies with it, so a durably stored job has nobody to answer. What
really has to be distributed is the **provider rate** (all nodes together must
stay under the provider limit) and the **queue bound**.

### Decision (proposed): B — Redis Streams
- Real cross-node asynchronous messaging, global queue depth and a global bound — the things Experiment 5 measures.
- No new infrastructure: Kanad doesn't need SQS/IAM in Terraform, and local dev stays `docker compose up`.
- Shares the Redis client, token bucket code (#5) and test harness (#29).
- **Fallback if it runs late:** option A behind the same `queue.Queue` interface (~1 day). The shared provider bucket — the part that matters most — is identical in both.

### How it works

```
request (cache miss) ──► take token from rl:provider:<model>
                           ├─ got token ──► call provider directly (fast path, no queue)
                           └─ no token, or provider answered 429
                                 │ XLEN queue:provider >= QUEUE_MAX ──► 503 queue_full
                                 ▼
                        XADD queue:provider {id, node, deadline, model, request}
                                 │
              workers on all nodes: XREADGROUP (group "workers")
                 ├─ deadline passed?  ──► XACK, drop (client already gave up)
                 ├─ wait for token from rl:provider:<model>
                 ├─ call provider (on 429: back off, retry until deadline)
                 ├─ XADD reply:<origin-node> {id, response|error}
                 └─ XACK
                                 │
origin node: one goroutine XREADs reply:<node-id>, hands the result to the
waiting handler by request id; handler gives up after QUEUE_WAIT_TIMEOUT ──► 504 queue_timeout
```

Details:
- **One reply reader per node**, not one `BLPOP` per waiting request — avoids exhausting the Redis connection pool when thousands of requests wait.
- **Queue bound:** `XLEN` check before `XADD`. Concurrent producers can overshoot `QUEUE_MAX` slightly; acceptable and documented. We do *not* use `XADD MAXLEN` because trimming would silently drop jobs we already accepted.
- **Deadlines** travel with the job so workers never spend money on a request whose client has timed out.
- **Crash recovery:** a background loop `XAUTOCLAIM`s entries pending longer than `QUEUE_VISIBILITY_TIMEOUT` (worker died mid-job). Reply streams get `MAXLEN ~1000` and an `EXPIRE`, so replies to dead nodes vanish.
- **Node id:** ECS task id (or hostname) at startup.

### Config

| Env var | Default | Meaning |
|---|---|---|
| `QUEUE_ENABLED` | `true` | off = upstream 429s go straight to the client (Exp. 5 baseline) |
| `QUEUE_MAX` | `1000` | global bound → `503 queue_full` |
| `QUEUE_WAIT_TIMEOUT_MS` | `30000` | client-side wait → `504 queue_timeout` |
| `QUEUE_WORKERS` | `8` | workers per node |
| `PROVIDER_RPS_<MODEL>` | `45` | shared provider bucket, set just below the provider's limit |
| `QUEUE_VISIBILITY_TIMEOUT_MS` | `60000` | when another worker reclaims a stuck job |

### Metrics
`queue_depth` (gauge, from `XLEN`), `queue_wait_seconds` (histogram), `queue_rejected_total{reason="full|timeout|expired"}`, `queue_jobs_total{result}`, `provider_tokens_wait_seconds`.

## 2. Team budgets

### Design
- **Key:** `budget:<team>:<yyyy-mm>` (UTC month), an **integer in micro-dollars** (`INCRBY`). Integers avoid float drift from thousands of `INCRBYFLOAT`s. `EXPIRE` ~40 days on first write.
- **Check** before the provider call: `GET` < limit, else `402 budget_exceeded`. Cache hits cost nothing, so they are served even when the team is over budget and are never charged. This means moving the budget check after the cache lookup in the handler (coordinate with Preshit, #30).
- **Spend** after the response: `INCRBY` actual cost from `usage` tokens × price.
- **Limits:** `TEAM_BUDGETS="team-a=50,team-b=10"` (USD/month), `TEAM_BUDGET_USD` as the default for unlisted teams (`0` = unlimited).
- **Redis down:** fail open by default (`BUDGET_FAIL_MODE=open|closed`), counted in `budget_errors_total`. Spend that fails to record is logged so it can be reconciled.

### The check-then-spend race
Check and spend are separate steps, so concurrent requests from one team can all
pass the check before any of them records its spend:

> **max over-spend ≈ (team's concurrent in-flight requests) × (max cost of one request)**

`max_tokens` caps the cost of one request. Example: 50 in flight × strong model ×
4k output tokens ≈ 50 × $0.04 = **$2** past the limit. We accept this for the MVP
and report it.

**Stretch — exact limits by reservation:** a Lua script atomically adds the
*estimated* cost if under the limit (`reserve`), and after the response adds
`actual − estimated` (`settle`). Over-spend is then bounded by estimation error
instead of concurrency. Worth a small experiment if time allows.

### Alerts (stretch)
When a spend crosses 80% (`old < 0.8·L ≤ new`) emit a log line and increment
`budget_alerts_total{team}`; Grafana alert rule on top.

### Metrics
`team_spend_usd_total{team}`, `requests_total{result="budget_exceeded"}`, `budget_errors_total`.

## 3. Redis keys added (see `docs/api.md`)

| Key | Type | Purpose |
|---|---|---|
| `rl:provider:<model>` | hash | shared provider-side token bucket (same script as per-key limits) |
| `queue:provider` | stream | pending jobs, consumer group `workers` |
| `reply:<node-id>` | stream | results for requests waiting on that node |
| `budget:<team>:<yyyy-mm>` | string (int) | micro-USD spent this month |
