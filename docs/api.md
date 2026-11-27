# API contract & key formats

Agree on this in Week 1 and change it only via PR (all four members review).

## Endpoint

`POST /v1/chat/completions` — OpenAI-compatible subset (see `internal/api/types.go`).

Request headers:

| Header          | Required | Meaning                                   |
|-----------------|----------|-------------------------------------------|
| `Authorization` | yes      | `Bearer <api-key>` — rate-limit identity  |
| `X-Team-ID`     | no       | budget identity (defaults to the API key) |
| `X-Request-ID`  | no       | propagated; generated if absent           |

Body: `{"model": "auto|cheap|strong", "messages": [...], "max_tokens": N, "no_cache": false}`

Response headers: `X-Cache: HIT|MISS|BYPASS|COALESCED` (`COALESCED`: waited for an identical request's provider call and paid nothing), `X-Model-Used`, `X-Cost-USD`, `X-Request-ID`.

Error body: `{"error": {"type": "...", "message": "..."}}`

| Status | type              | When                                      |
|--------|-------------------|-------------------------------------------|
| 400    | `bad_request`     | invalid body                              |
| 401    | `bad_request`     | missing API key                           |
| 402    | `budget_exceeded` | team over its monthly budget (cache hits are still served) |
| 429    | `rate_limited`    | per-key limit hit (has `Retry-After`)     |
| 502    | `upstream_error`  | provider failed after retries/fallback    |
| 503    | `queue_full`      | backpressure: overflow queue is full      |
| 503    | `limiter_unavailable` | Redis down and `RATELIMIT_FAIL_MODE=closed` (has `Retry-After`) |
| 503    | `budget_unavailable` | Redis down and `BUDGET_FAIL_MODE=closed` (has `Retry-After`) |
| 504    | `queue_timeout`   | waited in the queue longer than `QUEUE_WAIT_TIMEOUT_MS` |

Other endpoints: `GET /healthz`, `GET /metrics` (Prometheus).

## Redis key formats

| Key                               | Type   | Owner    | Notes                                  |
|-----------------------------------|--------|----------|----------------------------------------|
| `cache:<sha256(model,messages,max_tokens)>` | string | Dinesh | JSON `ChatResponse`, TTL = cache TTL; `max_tokens` is left out of the hash when not set |
| `lock:<cache-key>`                | string | Dinesh   | coalescing lock, short TTL             |
| `rl:<api-key>`                    | hash   | Saravjit | token bucket `{tokens, ts}`; `ts` is Redis server time (s); auto-expires when full |
| `rl:provider:<model>`             | hash   | Saravjit | shared provider-side token bucket      |
| `budget:<team>:<yyyy-mm>`         | string | Saravjit | integer **micro-USD** spent (`INCRBY`), UTC month |
| `queue:provider`                  | stream | Saravjit | overflow jobs, consumer group `workers` |
| `reply:<node-id>`                 | stream | Saravjit | results for requests waiting on that node |

Design and rationale: [`docs/design/queue-and-budgets.md`](design/queue-and-budgets.md).

## Structured log line (one per request)

```json
{"msg":"request","request_id":"…","api_key":"****abcd","team":"…","model":"cheap",
 "cache":"HIT","tokens":188,"cost_usd":0,"cost_saved_usd":0.000055,"latency_ms":12}
```

## Environment variables (gateway)

`PORT`, `PROVIDER_URL`, `PROVIDER_API_KEY`, `PROVIDER_TIMEOUT_MS` (one provider attempt, default 30000),
`REQUEST_TIMEOUT_MS` (whole direct provider step, default 60000, must be >= `PROVIDER_TIMEOUT_MS`),
`SHUTDOWN_TIMEOUT_MS` (time to drain in-flight requests on SIGTERM, default 20000), `REDIS_ADDR`,
`RATE_LIMIT_PER_MIN`, `RATE_LIMIT_BURST`, `RATELIMIT_BACKEND` (`local`|`redis`, default `redis` when `REDIS_ADDR` is set),
`RATELIMIT_FAIL_MODE` (`open`|`closed`, default `open`), `RATELIMIT_REDIS_TIMEOUT_MS` (default 50), `TEAM_BUDGET_USD` (default monthly cap, 0 = unlimited), `TEAM_BUDGETS` (`team-a=50,team-b=10`),
`BUDGET_BACKEND` (`memory`|`redis`), `BUDGET_ALERT_FRACTION` (default 0.8), `BUDGET_FAIL_MODE` (`open`|`closed`),
`BUDGET_REDIS_TIMEOUT_MS` (default 50), `CACHE_ENABLED` (default `true`; `false` sends every request to the provider as `BYPASS`), `CACHE_TTL_SECONDS` (default 300),
`CACHE_COALESCE` (`off`|`local`|`redis`, default `off`; see below), `CACHE_COALESCE_WAIT_MS` (default 5000), `CACHE_COALESCE_POLL_MS` (default 50),
`CACHE_BACKEND` (`memory`|`redis`, default `redis` when `REDIS_ADDR` is set), `CACHE_REDIS_TIMEOUT_MS` (default 50; a slower lookup counts as a miss),
`ROUTER_MAX_CHEAP_CHARS`,
`QUEUE_ENABLED` (default `true`), `QUEUE_BACKEND` (`memory`|`redis`), `QUEUE_MAX` (1000), `QUEUE_WAIT_TIMEOUT_MS` (30000),
`QUEUE_WORKERS` (8 per node), `QUEUE_VISIBILITY_TIMEOUT_MS` (wait + 30s), `PROVIDER_RPS` (45; 0 = no pacing),
`PROVIDER_BURST`, `PROVIDER_RPS_PER_MODEL` (`false`), `NODE_ID`,
`RETRY_MAX_ATTEMPTS` (3 attempts per model, including the first), `RETRY_BASE_BACKOFF_MS` (100),
`RETRY_MAX_BACKOFF_MS` (2000), `BREAKER_FAILURE_THRESHOLD` (5 consecutive failures open a model's breaker),
`BREAKER_OPEN_MS` (10000, then one probe request is let through), `FALLBACK_ENABLED` (default `true`; `false` for the Experiment 6 baseline).

### Retries, circuit breaker, fallback

Both the direct path and the queue workers call the provider through `provider.Resilient`:

- 5xx, timeouts and connection errors are retried with exponential backoff and full jitter
  (`rand[0, min(max, base*2^n)]`), never waiting past the request's deadline. 4xx is not retried.
  A provider 429 is returned unchanged so the overflow queue handles it.
- Each model has a circuit breaker (closed → open → half-open). Only the failures above count;
  4xx, 429 and a client that went away do not.
- When `cheap` or `strong` fails (retries used up or breaker open), the request is served by the other one.
  `X-Model-Used` and `X-Cost-USD` show the model that actually answered, and a fallback response is not cached.
  Other model names are retried but have no fallback. If both fail the client gets 502 `upstream_error`.
- For `cheap`/`strong` the response body's `model` is `cheap`/`strong` (the gateway's name, not the provider's).

| Metric | Meaning |
|---|---|
| `provider_retries_total{model}` | retry attempts (not counting the first try) |
| `provider_fallbacks_total{from,to}` | requests served by the other model |
| `breaker_state{model}` | gauge: 0 closed, 1 open, 2 half-open |
| `breaker_opens_total{model}` | times the breaker opened |

Request coalescing (`CACHE_COALESCE`): on a miss, identical requests that are in flight at the same
time share one provider call. `local` groups them inside one node; `redis` also takes a `lock:<cache-key>`
(TTL `REQUEST_TIMEOUT_MS` + 1 s) so only one node in the cluster calls the provider, while the others poll
the cache. It needs `CACHE_BACKEND=redis` and `CACHE_TTL_SECONDS` > 0. A waiter that times out, or a Redis
error, means the request calls the provider itself: coalescing can add latency but never fails a request.
Design: [`docs/design/cache-coalescing.md`](design/cache-coalescing.md).

The cache fails open: if Redis is slow or down, the request is served as a `MISS` from the provider and
`llmgw_cache_errors_total{op="get"|"set"}` is incremented.

Cache metrics (all prefixed `llmgw_`):

| Metric | Meaning |
|---|---|
| `cache_lookups_total{result="hit"\|"miss"\|"error"}` | one per lookup (an error is served as a miss) |
| `cache_errors_total{op="get"\|"set"}` | Redis slow or down |
| `cache_lookup_seconds_bucket{le}`, `_sum`, `_count` | lookup latency histogram |
| `cost_saved_usd_total{model}` | on a hit or coalesced request, what the provider would have charged for that response's usage |
| `cache_coalesced_total{level="local"\|"redis"}` | requests answered by waiting for an identical request on this node (`local`) or another node (`redis`) |
| `cache_coalesce_fallback_total` | waiters that gave up after `CACHE_COALESCE_WAIT_MS` and called the provider themselves |
| `cache_errors_total{op="lock"}` | Redis lock errors (the request skips the lock and calls the provider) |

Example queries: hit rate `sum(rate(llmgw_cache_lookups_total{result="hit"}[1m])) / sum(rate(llmgw_cache_lookups_total[1m]))`,
dollars saved `sum(llmgw_cost_saved_usd_total)`, lookup p95
`histogram_quantile(0.95, sum by (le) (rate(llmgw_cache_lookup_seconds_bucket[1m])))`.
These panels are in the Grafana dashboard "LLM Gateway: Cache"
(`deploy/grafana/provisioning/dashboards/cache.json`, loaded automatically by `docker compose up`).

Shared Redis client (`internal/redisx`, one pool for all components; unset means go-redis default):
`REDIS_POOL_SIZE` (default 10 per CPU), `REDIS_DIAL_TIMEOUT_MS` (5000), `REDIS_READ_TIMEOUT_MS` (3000),
`REDIS_WRITE_TIMEOUT_MS` (same as read), `REDIS_MAX_RETRIES` (default 0: a failed command is not retried, so a down Redis fails in about 1 ms and each component's fail-open or fail-closed rule applies at once). Per-call context timeouts (e.g. `RATELIMIT_REDIS_TIMEOUT_MS`) are honoured.
