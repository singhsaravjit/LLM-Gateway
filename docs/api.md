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

Other endpoints: `GET /healthz`, `GET /metrics` (Prometheus).

## Redis key formats

| Key                               | Type   | Owner    | Notes                                  |
|-----------------------------------|--------|----------|----------------------------------------|
| `cache:<sha256(model,messages)>`  | string | Dinesh   | JSON `ChatResponse`, TTL = cache TTL   |
| `lock:<cache-key>`                | string | Dinesh   | coalescing lock, short TTL             |
| `rl:<api-key>`                    | hash   | Saravjit | token bucket `{tokens, ts}`            |
| `budget:<team>:<yyyy-mm>`         | string | Saravjit | `INCRBYFLOAT` USD spent                |
| `queue:provider`                  | stream | Saravjit | if Redis Streams chosen over SQS       |

## Structured log line (one per request)

```json
{"msg":"request","request_id":"…","api_key":"****abcd","team":"…","model":"cheap",
 "cache":"HIT","tokens":188,"cost_usd":0.000055,"latency_ms":12}
```

## Environment variables (gateway)

`PORT`, `PROVIDER_URL`, `PROVIDER_API_KEY`, `PROVIDER_TIMEOUT_MS`, `REDIS_ADDR`,
`RATE_LIMIT_PER_MIN`, `RATE_LIMIT_BURST`, `TEAM_BUDGET_USD`, `CACHE_TTL_SECONDS`,
`ROUTER_MAX_CHEAP_CHARS`.
