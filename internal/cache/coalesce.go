package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"github.com/singhsaravjit/llm-gateway/internal/api"
)

// CoalesceMode picks how identical in-flight requests are grouped.
type CoalesceMode string

const (
	CoalesceOff   CoalesceMode = "off"   // every miss calls the provider
	CoalesceLocal CoalesceMode = "local" // one provider call per key per node
	CoalesceRedis CoalesceMode = "redis" // one provider call per key per cluster
)

func ParseCoalesceMode(s string) (CoalesceMode, error) {
	switch m := CoalesceMode(s); m {
	case "":
		return CoalesceOff, nil
	case CoalesceOff, CoalesceLocal, CoalesceRedis:
		return m, nil
	}
	return "", fmt.Errorf("unknown CACHE_COALESCE %q (want off|local|redis)", s)
}

// Outcome says how a coalesced call got its answer.
type Outcome int

const (
	// Fetched: this request called the provider (and pays for it).
	Fetched Outcome = iota
	// SharedLocal: another request on this node called the provider.
	SharedLocal
	// SharedRedis: another node called the provider; the answer came from the cache.
	SharedRedis
)

// FetchFunc calls the provider and stores the answer in the cache. It must
// store before returning, so waiters on other nodes can find it.
type FetchFunc func(ctx context.Context) (*api.ChatResponse, error)

// Coalescer makes sure only one provider call per cache key is in flight.
// Design and tradeoffs: docs/design/cache-coalescing.md.
//
// Local: golang.org/x/sync/singleflight. Concurrent calls with the same key
// wait for the first one and share its answer.
//
// Redis: on top of that, the one call per node tries
// SET lock:<key> <token> NX PX <lockTTL>. The winner fetches and then deletes
// the lock (only if it still holds it). The others poll the cache every
// pollEvery until the answer appears or wait runs out, then fetch themselves.
// A slow or dead lock holder costs latency, never a failed request, and a
// Redis error skips the lock. This matches the cache's fail-open rule.
//
// Metrics: cache_coalesced_total{level="local"|"redis"},
// cache_coalesce_fallback_total, cache_errors_total{op="lock"}.
type Coalescer struct {
	mode      CoalesceMode
	group     singleflight.Group
	client    redis.Cmdable // Redis mode only
	cache     Cache         // polled by waiters; pass the uninstrumented cache
	rec       Recorder
	lockTTL   time.Duration
	wait      time.Duration
	pollEvery time.Duration
	timeout   time.Duration // per Redis call
}

type CoalesceOption func(*Coalescer)

// WithRedisLock enables cross-node coalescing (mode redis). lockTTL should be
// a bit above the longest provider call (REQUEST_TIMEOUT_MS), so a crashed
// holder frees the key.
func WithRedisLock(client redis.Cmdable, lockTTL time.Duration) CoalesceOption {
	return func(c *Coalescer) { c.client, c.lockTTL = client, lockTTL }
}

// WithWait sets how long a waiter polls for another node's answer before it
// calls the provider itself (default 5 s), and the poll interval (default 50 ms).
func WithWait(wait, pollEvery time.Duration) CoalesceOption {
	return func(c *Coalescer) { c.wait, c.pollEvery = wait, pollEvery }
}

// WithLockTimeout sets the per-call budget for lock commands (default 50 ms).
func WithLockTimeout(d time.Duration) CoalesceOption { return func(c *Coalescer) { c.timeout = d } }

func NewCoalescer(mode CoalesceMode, cache Cache, rec Recorder, opts ...CoalesceOption) (*Coalescer, error) {
	c := &Coalescer{mode: mode, cache: cache, rec: rec,
		wait: 5 * time.Second, pollEvery: 50 * time.Millisecond, timeout: 50 * time.Millisecond}
	for _, o := range opts {
		o(c)
	}
	if mode == CoalesceRedis && c.client == nil {
		return nil, fmt.Errorf("CACHE_COALESCE=redis requires a Redis client")
	}
	for _, n := range []string{`cache_coalesced_total{level="local"}`, `cache_coalesced_total{level="redis"}`,
		"cache_coalesce_fallback_total", `cache_errors_total{op="lock"}`} {
		rec.Inc(n, 0)
	}
	return c, nil
}

func (c *Coalescer) Mode() CoalesceMode { return c.mode }

type result struct {
	resp      *api.ChatResponse
	fromRedis bool // the answer was fetched by another node
}

// Do returns the answer for key, calling fetch at most once per key at a time
// (per node, or per cluster in Redis mode). The caller's ctx only limits how
// long this request waits: the shared fetch keeps going if the first caller
// goes away, because other requests may be waiting on it.
func (c *Coalescer) Do(ctx context.Context, key string, fetch FetchFunc) (*api.ChatResponse, Outcome, error) {
	if c.mode == CoalesceOff {
		resp, err := fetch(ctx)
		return resp, Fetched, err
	}
	var ran atomic.Bool // set only in the request whose function runs
	ch := c.group.DoChan(key, func() (any, error) {
		ran.Store(true)
		return c.lead(context.WithoutCancel(ctx), key, fetch)
	})
	select {
	case <-ctx.Done():
		return nil, Fetched, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return nil, Fetched, r.Err
		}
		res := r.Val.(result)
		switch {
		case res.fromRedis:
			c.rec.Inc(`cache_coalesced_total{level="redis"}`, 1)
			return res.resp, SharedRedis, nil
		case !ran.Load():
			c.rec.Inc(`cache_coalesced_total{level="local"}`, 1)
			return res.resp, SharedLocal, nil
		}
		return res.resp, Fetched, nil
	}
}

// lead runs once per key per node.
func (c *Coalescer) lead(ctx context.Context, key string, fetch FetchFunc) (any, error) {
	if c.mode != CoalesceRedis {
		resp, err := fetch(ctx)
		return result{resp: resp}, err
	}
	lock, token := "lock:"+key, newToken()
	got, err := c.acquire(ctx, lock, token)
	if err != nil { // Redis down or slow: no lock, just fetch
		c.rec.Inc(`cache_errors_total{op="lock"}`, 1)
		resp, err := fetch(ctx)
		return result{resp: resp}, err
	}
	if got {
		defer c.release(ctx, lock, token)
		// Another node may have stored the answer and released its lock
		// between our cache miss and our SET NX.
		if resp, hit, _ := c.cache.Get(ctx, key); hit {
			return result{resp: resp, fromRedis: true}, nil
		}
		resp, err := fetch(ctx)
		return result{resp: resp}, err
	}
	if resp := c.waitFor(ctx, key); resp != nil {
		return result{resp: resp, fromRedis: true}, nil
	}
	c.rec.Inc("cache_coalesce_fallback_total", 1)
	resp, err := fetch(ctx)
	return result{resp: resp}, err
}

// waitFor polls the cache until the answer appears or the wait runs out.
func (c *Coalescer) waitFor(ctx context.Context, key string) *api.ChatResponse {
	deadline := time.NewTimer(c.wait)
	defer deadline.Stop()
	tick := time.NewTicker(c.pollEvery)
	defer tick.Stop()
	for {
		select {
		case <-deadline.C:
			return nil
		case <-tick.C:
			if resp, hit, _ := c.cache.Get(ctx, key); hit {
				return resp
			}
		}
	}
}

func (c *Coalescer) acquire(ctx context.Context, lock, token string) (bool, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()
	return c.client.SetNX(ctx, lock, token, c.lockTTL).Result()
}

// releaseScript deletes the lock only if it still holds our token, so a slow
// holder whose lock already expired cannot delete another node's lock.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`)

func (c *Coalescer) release(ctx context.Context, lock, token string) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()
	// On error the lock simply expires after lockTTL.
	if err := releaseScript.Run(ctx, c.client, []string{lock}, token).Err(); err != nil {
		c.rec.Inc(`cache_errors_total{op="lock"}`, 1)
	}
}

func (c *Coalescer) callCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.timeout)
}

func newToken() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
