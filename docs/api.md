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

Response headers: `X-Cache: HIT|MISS|BYPASS`, `X-Model-Used`, `X-Cost-USD`, `X-Request-ID`.

Error body: `{"error": {"type": "...", "message": "..."}}`

| Status | type              | When                                      |
|--------|-------------------|-------------------------------------------|
| 400    | `bad_request`     | invalid body                              |
| 401    | `bad_request`     | missing API key                           |
| 402    | `budget_exceeded` | team over budget                          |
| 429    | `rate_limited`    | per-key limit hit (has `Retry-After`)     |
| 502    | `upstream_error`  | provider failed after retries/fallback    |
| 503    | `queue_full`      | backpressure: overflow queue is full      |
| 503    | `limiter_unavailable` | Redis down and `RATELIMIT_FAIL_MODE=closed` (has `Retry-After`) |
| 504    | `queue_timeout`   | waited in the queue longer than `QUEUE_WAIT_TIMEOUT_MS` |

Other endpoints: `GET /healthz`, `GET /metrics` (Prometheus).

## Redis key formats

| Key                               | Type   | Owner    | Notes                                  |
|-----------------------------------|--------|----------|----------------------------------------|
| `cache:<sha256(model,messages)>`  | string | Dinesh   | JSON `ChatResponse`, TTL = cache TTL   |
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
 "cache":"HIT","tokens":188,"cost_usd":0.000055,"latency_ms":12}
```

## Environment variables (gateway)

`PORT`, `PROVIDER_URL`, `PROVIDER_API_KEY`, `PROVIDER_TIMEOUT_MS`, `REDIS_ADDR`,
`RATE_LIMIT_PER_MIN`, `RATE_LIMIT_BURST`, `RATELIMIT_BACKEND` (`local`|`redis`, default `redis` when `REDIS_ADDR` is set),
`RATELIMIT_FAIL_MODE` (`open`|`closed`, default `open`), `RATELIMIT_REDIS_TIMEOUT_MS` (default 50), `TEAM_BUDGET_USD`, `CACHE_TTL_SECONDS`,
`ROUTER_MAX_CHEAP_CHARS`.

Shared Redis client (`internal/redisx`, one pool for all components; unset means go-redis default):
`REDIS_POOL_SIZE` (default 10 per CPU), `REDIS_DIAL_TIMEOUT_MS` (5000), `REDIS_READ_TIMEOUT_MS` (3000),
`REDIS_WRITE_TIMEOUT_MS` (same as read). Per-call context timeouts (e.g. `RATELIMIT_REDIS_TIMEOUT_MS`) are honoured.
