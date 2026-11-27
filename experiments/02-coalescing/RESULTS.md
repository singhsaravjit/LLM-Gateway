# Experiment 2: Request coalescing (local)

Owner: Dinesh · Issues #13, #15 · Run on Nov 26, 2026 on my laptop (Apple Silicon), on main
with the overflow queue (#40) and budgets (#39), plus coalescing (#13).

The baseline (no coalescing) was first run on Nov 22 (`results/baseline_no_coalescing_n*.json`).
This run adds coalescing (#13) and a second gateway node. Design notes:
`docs/design/cache-coalescing.md`.

## Question

When many identical requests arrive at the same moment and the answer is not cached
yet, how many of them reach the provider? How much does each coalescing mode cut that,
and what does it cost in latency?

## Setup

- Gateway built from the coalescing branch, Redis 7 in Docker, mock LLM with
  800 ms ± 300 ms latency. Rate limits set very high in the gateway and the mock, so
  every request is a cache result, not a 429.
- The overflow queue is on with its defaults (`PROVIDER_RPS=45`, shared by all nodes,
  8 workers per node), so provider calls above 45 per second wait in the queue.
- `burst.py` sends N identical requests at the same instant (threads wait on a barrier),
  with a new prompt each time so the key starts cold. With 2 nodes the requests are split
  round robin across both gateways.
- Coalescing modes (`CACHE_COALESCE`): `off`, `local` (singleflight inside each node),
  `redis` (singleflight plus a `lock:<key>` in Redis, so one node in the cluster calls
  the provider and the others poll the cache every 50 ms).

Reproduce (needs Redis: `docker compose up -d redis`):

```bash
experiments/02-coalescing/run.sh
```

## Results

Provider calls per burst (ideal is 1):

| Mode | Nodes | N = 10 | N = 50 | N = 200 |
|---|---|---|---|---|
| off | 1 | 10 | 50 | 200 |
| local | 1 | **1** | **1** | **1** |
| redis | 1 | **1** | **1** | **1** |
| off | 2 | 10 | 50 | 200 |
| local | 2 | 2 | 2 | 2 |
| redis | 2 | **1** | **1** | **1** |

Burst latency, p50 (min to max):

| Mode | Nodes | N = 10 | N = 50 | N = 200 |
|---|---|---|---|---|
| off | 1 | 797 ms (635 to 1,089) | 804 ms (565 to 1,191) | **5,570 ms (583 to 15,476)** |
| local | 1 | 1,095 ms (1,094 to 1,095) | 1,122 ms (1,121 to 1,123) | 602 ms (594 to 608) |
| redis | 1 | 676 ms (675 to 677) | 852 ms (851 to 854) | 997 ms (980 to 1,004) |
| off | 2 | 856 ms (631 to 1,094) | 811 ms (540 to 1,183) | **3,320 ms (555 to 8,401)** |
| local | 2 | 877 ms (781 to 974) | 1,087 ms (1,080 to 1,093) | 714 ms (587 to 839) |
| redis | 2 | 620 ms (616 to 624) | 704 ms (684 to 723) | 575 ms (554 to 607) |

All requests returned 200 in every run. Requests that waited are marked
`X-Cache: COALESCED` and paid $0. Raw output in `results/`.

## What I learned

- **Without coalescing, one answer costs N provider calls.** Every request in the burst
  missed the cache, because the answer did not exist yet. 200 identical requests meant
  200 calls and 200 slots of the provider's rate limit.
- **`local` fixes it on one node, but not across nodes.** On 1 node every burst became
  1 call. On 2 nodes it became 2 calls, one per node. With the 4 to 8 nodes we plan on
  AWS, that is still 4 to 8 calls per burst.
- **`redis` gives 1 call for the whole cluster.** On 2 nodes, the node that got the lock
  called the provider, and the other node waited for the answer in Redis.
- **Without coalescing, big bursts also wait in the queue.** 200 calls is more than the
  provider allows per second (45), so the overflow queue held the extra calls. The
  burst p50 went to 5.6 s and the slowest request took 15.5 s on 1 node. With
  coalescing the same burst is 1 call, never queues, and finishes in about 0.6 to 1 s.
  The two features work together: coalescing removes duplicate calls, and the queue
  handles the rest.
- **Waiting costs about one provider call, not more.** With coalescing, all requests in
  a burst finish at almost the same time (the min-to-max spread is a few ms to about
  250 ms), because they all wait for the same call. The p50 is about one provider call
  (575 to 1,122 ms, since the mock latency is random). On 2 nodes with `redis`, waiters on the other node
  pay up to one extra poll interval (50 ms) on top of that.
- **Redis traffic stays small.** Each node sends at most one lock attempt per key, then
  one cache read per 50 ms while it waits. A burst of 200 on 2 nodes meant about 2 lock
  calls, not 200.

## Failure cases (covered by tests in `internal/cache/coalesce_test.go`)

- **Lock holder dies:** waiters poll until `CACHE_COALESCE_WAIT_MS` (default 5 s), then
  call the provider themselves and count `cache_coalesce_fallback_total`. The dead lock
  expires after `REQUEST_TIMEOUT_MS` + 1 s.
- **Redis down:** the lock is skipped (counted in `cache_errors_total{op="lock"}`) and
  local coalescing still applies.
- **Provider error:** all waiters get the same error, and the provider is called once.
- **A waiter's client disconnects:** only that request stops; the shared call continues
  for the others.

## Limits of these numbers

- One laptop, two nodes, one burst per setting. The provider latency is random
  (800 ± 300 ms), so the p50 values move by a few hundred ms from run to run. Provider
  call counts are exact and did not vary.
- Bursts arrive at exactly the same instant. In real traffic, identical requests arrive
  spread out over the provider latency. Coalescing still applies to everything that
  arrives while the first call is in flight.

## Still to do (Week 3, AWS)

- Repeat with 2 and 4 nodes behind the ALB.
- Kill the lock holder mid-call and measure the fallback latency.
- Steady load (Locust, Zipf prompts, short TTL) with coalescing `off` vs `redis`, to
  see how often stampedes happen when popular entries expire.
