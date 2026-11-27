# Design notes: request coalescing

Owner: Dinesh · Issues #4 (notes), #13 (build), #15 (Experiment 2) · Status: **built in #13** (`internal/cache/coalesce.go`)

## The problem

The cache only helps after the first answer is stored. If 50 identical prompts arrive
at the same moment and the key is not cached yet, all 50 miss and all 50 call the
provider. That is 50 times the cost and 50 slots of the provider's rate limit for one
answer. This is called a cache stampede (or thundering herd). It is most likely right
after a popular entry expires, or when a new prompt suddenly gets popular.

The goal: for one cache key, only one provider call is in flight at a time. Everyone
else waits for that answer.

## Option A: `singleflight` inside each node

`golang.org/x/sync/singleflight` groups concurrent calls with the same key inside one
process. The first caller runs the provider call; the others block and get the same
result.

- No Redis work, no network hops, almost no added latency.
- Only works per node. With N gateway nodes behind the ALB, identical requests spread
  across nodes, so the provider still sees up to N calls per burst.
- Nothing to clean up if a node dies: its waiters die with it.

## Option B: Redis lock `lock:<cache-key>` across nodes

The key format is already reserved in `docs/api.md`.

```
miss on cache:<h>
  SET lock:cache:<h> <node-id>:<request-id> NX PX <lock-ttl>
  ├─ got the lock -> call provider -> SET cache:<h> -> release lock (only if still ours)
  └─ lock taken   -> poll GET cache:<h> every ~50 ms until wait timeout
                       ├─ found   -> serve as HIT (count as coalesced)
                       └─ timeout -> call provider ourselves (fail open)
```

- One provider call per burst for the whole cluster.
- Costs 1 to 2 extra Redis calls per miss, plus polling for waiters.
- The lock needs a TTL a bit above the provider timeout, so a crashed holder cannot block
  a key forever.
- Release with a small Lua script that deletes the lock only if its value is still ours,
  so a slow holder whose lock already expired cannot delete someone else's lock.
- Waiters fall back to calling the provider after the wait timeout. A slow or dead lock
  holder costs extra latency, never a failed request. This matches the cache's fail-open
  rule.
- If Redis is down, skip the lock and just call the provider.

## Plan for #13: use both

1. `singleflight` first, so identical requests on the same node collapse for free.
2. Then the Redis lock, so only one node calls the provider.

Each node sends at most one lock attempt per key, which keeps Redis traffic small even
for big bursts.

Config: `CACHE_COALESCE` (`off` | `local` | `redis`), `CACHE_COALESCE_WAIT_MS` (how long
waiters poll before giving up), lock TTL derived from `REQUEST_TIMEOUT_MS` (the longest a provider call can take).

Metrics: `cache_coalesced_total{level="local"|"redis"}` (requests answered by waiting),
`cache_coalesce_fallback_total` (waiters that gave up and called the provider), plus the
provider call count.

## Experiment 2

Fire bursts of N identical requests (N = 10, 50, 200) at a cold key, with coalescing
`off`, `local`, `redis`, on 1 node and on 2+ nodes.

Measure: provider calls per burst (ideal is 1), p50/p99 latency of the burst, errors.
Also kill the lock holder mid-call to show waiters fall back after the wait timeout.

Expected: `off` gives N calls. `local` gives about one per node. `redis` gives 1. Waiters
pay roughly the provider latency plus up to one poll interval.
