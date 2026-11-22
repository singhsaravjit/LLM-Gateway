package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/singhsaravjit/llm-gateway/internal/redisx/redistest"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, c := redistest.Start(t)
	mr.SetTime(time.Unix(1_700_000_000, 0))
	return mr, c
}

func TestRedisBurstThenRefill(t *testing.T) {
	mr, c := newTestRedis(t)
	l := NewRedis(c, 60, 3) // 1 token/s, burst 3
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		d, err := l.Allow(ctx, "k")
		if err != nil || !d.Allowed {
			t.Fatalf("request %d: allowed=%v err=%v", i, d.Allowed, err)
		}
	}
	d, err := l.Allow(ctx, "k")
	if err != nil || d.Allowed {
		t.Fatalf("4th request should be limited: allowed=%v err=%v", d.Allowed, err)
	}
	if d.RetryAfter <= 0 || d.RetryAfter > time.Second {
		t.Fatalf("retry-after = %v, want (0, 1s]", d.RetryAfter)
	}

	mr.SetTime(time.Unix(1_700_000_001, 0))
	if d, _ := l.Allow(ctx, "k"); !d.Allowed {
		t.Fatal("should refill after 1s")
	}
}

func TestRedisKeysIndependentAndExpire(t *testing.T) {
	mr, c := newTestRedis(t)
	l := NewRedis(c, 60, 1)
	ctx := context.Background()
	if d, _ := l.Allow(ctx, "a"); !d.Allowed {
		t.Fatal("a should be allowed")
	}
	if d, _ := l.Allow(ctx, "b"); !d.Allowed {
		t.Fatal("b has its own bucket")
	}
	if !mr.Exists("rl:a") {
		t.Fatal("expected key rl:a")
	}
	if ttl := mr.TTL("rl:a"); ttl <= 0 {
		t.Fatalf("bucket key should expire, ttl=%v", ttl)
	}
}

// The property Experiment 3 measures: N gateway nodes sharing Redis admit
// exactly the limit in total, while N local limiters admit N x the limit.
func TestNoOverAdmissionAcrossNodes(t *testing.T) {
	_, c := newTestRedis(t)
	const nodes, perNode, burst = 8, 50, 20
	ctx := context.Background()

	count := func(mk func() Limiter) int64 {
		var allowed atomic.Int64
		var wg sync.WaitGroup
		for n := 0; n < nodes; n++ {
			lim := mk()
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perNode; i++ {
					if d, _ := lim.Allow(ctx, "shared-key"); d.Allowed {
						allowed.Add(1)
					}
				}
			}()
		}
		wg.Wait()
		return allowed.Load()
	}

	// Frozen miniredis clock => no refill during the test, so exactly `burst` may pass.
	if got := count(func() Limiter { return NewRedis(c, 1, burst) }); got != burst {
		t.Fatalf("redis limiter admitted %d across %d nodes, want exactly %d", got, nodes, burst)
	}
	if got := count(func() Limiter { return NewLocal(1, burst) }); got <= burst {
		t.Fatalf("local limiters admitted %d, expected over-admission (> %d)", got, burst)
	}
}

func TestFailModes(t *testing.T) {
	mr, c := newTestRedis(t)
	mr.Close() // Redis down
	ctx := context.Background()

	d, err := NewRedis(c, 60, 10, WithFailMode(FailOpen)).Allow(ctx, "k")
	if err == nil || !d.Allowed {
		t.Fatalf("fail-open: want allowed + error, got allowed=%v err=%v", d.Allowed, err)
	}
	d, err = NewRedis(c, 60, 10, WithFailMode(FailClosed)).Allow(ctx, "k")
	if err == nil || d.Allowed {
		t.Fatalf("fail-closed: want rejected + error, got allowed=%v err=%v", d.Allowed, err)
	}
}

func TestSlowRedisTimesOut(t *testing.T) {
	_, c := newTestRedis(t)
	l := NewRedis(c, 60, 10, WithTimeout(time.Nanosecond), WithFailMode(FailClosed))
	d, err := l.Allow(context.Background(), "k")
	if err == nil || d.Allowed {
		t.Fatalf("expected timeout error and fail-closed rejection, got allowed=%v err=%v", d.Allowed, err)
	}
}

func TestParseFailMode(t *testing.T) {
	for in, want := range map[string]FailMode{"": FailOpen, "open": FailOpen, "closed": FailClosed} {
		if got, err := ParseFailMode(in); err != nil || got != want {
			t.Errorf("ParseFailMode(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := ParseFailMode("maybe"); err == nil {
		t.Error("expected error for unknown mode")
	}
}
