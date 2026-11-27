# Experiment 1: Caching (preliminary, local)

Owner: Dinesh · Issue #15 · Run on Nov 22, 2026 on my laptop (Apple Silicon), one gateway node.

These are first numbers for the HW9 proposal. The full experiment runs on AWS in Week 3.

## Question

How much does the shared Redis cache cut provider calls, cost and latency, how does
the TTL change that, and what does the cache cost when it never helps?

## Setup

- Gateway built from the Redis cache branch (PR #37), Redis 7 in Docker, mock LLM.
- Mock provider latency 800 ms ± 300 ms (the "strong" model is 2.5 times slower).
- Locust, 20 users, 2 minutes per run, each user waits 0.1 to 0.5 s between requests.
- Prompt mix from `loadtest/locustfile.py`: 1,000 prompts picked with a Zipf-like
  skew (`ZIPF_S=1.1`), so a few prompts repeat a lot. 10% of requests are long prompts.
- Rate limits off in the gateway and the mock, so every request is a cache result,
  not a 429. Redis is emptied before each run. These runs predate the overflow queue
  (#40); `run.sh` now sets `QUEUE_ENABLED=false` so reruns measure the same thing.
- I changed one thing per run. Exact settings are in `results/<run>/config.txt`.

Reproduce (on AWS, set `TARGET` and `METRICS_URLS`; see the top of `run.sh`):

```bash
experiments/01-caching/run.sh cache_off NO_CACHE=1
experiments/01-caching/run.sh cache_on_ttl300 CACHE_TTL_SECONDS=300
experiments/01-caching/run.sh cache_on_ttl30 CACHE_TTL_SECONDS=30
experiments/01-caching/run.sh worst_unique CACHE_TTL_SECONDS=300 UNIQUE_PROMPTS=1
```

## Results

| Run | Requests in 2 min | Req/s | Hit rate | Calls to provider | Cost per 1,000 requests | p50 latency | p95 latency |
|---|---|---|---|---|---|---|---|
| Cache off (baseline) | 1,915 | 16.0 | n/a | 1,915 (100%) | $0.287 | 850 ms | 2,000 ms |
| Cache on, TTL 300 s | 4,745 | 39.6 | **82.5%** | 832 (17.5%) | **$0.093 (67% less)** | **6 ms** | 1,100 ms |
| Cache on, TTL 30 s | 3,829 | 32.0 | 70.2% | 1,143 (29.9%) | $0.145 (50% less) | 7 ms | 1,400 ms |
| Worst case: every prompt unique | 1,914 | 16.0 | 0% | 1,914 (100%) | $0.303 | 840 ms | 1,900 ms |

Latency is what the Locust clients saw. 0 failed requests and 0 cache errors in every run.

Hits vs misses, from the gateway's own per-request log (TTL 300 s run):

| | p50 | p95 | p99 |
|---|---|---|---|
| Cache hit | 2 ms | 4 ms | 6 ms |
| Cache miss (goes to the provider) | 896 ms | 2,497 ms | 2,713 ms |

Redis memory: 821 cached responses used about 0.35 MB, roughly 450 bytes each.

## What I learned

- **The cache removes most provider calls.** With a 300 s TTL, 82.5% of requests never
  reached the provider. Cost per 1,000 requests dropped from $0.287 to $0.093. For the
  4,745 requests in that run, that is $0.92 saved out of $1.36.
- **A hit is about 400 times faster than a miss** (2 ms vs 896 ms at p50). Because users
  were not waiting on the provider, the same 20 users sent 2.5 times more requests.
- **TTL is a real tradeoff.** Going from 300 s to 30 s dropped the hit rate from 82.5% to
  70.2% and the savings from 67% to 50%. A longer TTL saves more but can serve an older
  answer, and keeps more entries in Redis.
- **p95 stays high with the cache on** (1,100 ms) because the slowest 5% are misses,
  mostly long prompts routed to the strong model. The cache helps the common case,
  not the tail.
- **Worst case costs almost nothing extra.** With every prompt unique the cache never
  hits, so every request pays one Redis lookup and one write. Latency (840 ms p50) was
  the same as with the cache off (850 ms): about 1 ms of Redis time is lost in 800 ms of
  provider time. Cost per request was 6% higher, but that is because the unique prompts
  are a few tokens longer (each gets a random ID), not because of the cache. The real
  cost of the worst case is Redis memory filling with entries nobody reads again
  (1,923 keys after 2 minutes).

## Limits of these numbers

- Two minutes is short. The cache starts empty, so part of each run is warm-up. Longer
  runs will show a higher hit rate.
- One node on a laptop. The point of Redis is that nodes share the cache. I checked that
  by hand (a response cached through one gateway is a hit on another), but have not
  measured it under load yet.
- The hit rate depends heavily on the prompt mix. Real traffic may repeat less than this
  Zipf mix.

## Still to do (Week 3, AWS)

- Repeat with 2 and 4 gateway nodes: shared Redis cache vs per-node memory cache.
- More TTLs (30 s, 300 s, 3600 s) and prompt mixes (`ZIPF_S`, `N_PROMPTS`).
- Longer runs, and Redis CPU and memory from CloudWatch.
- Real "cost saved" counter per hit, once #14 adds `cost_saved_usd_total`.
