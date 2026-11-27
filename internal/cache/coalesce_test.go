package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
	"github.com/singhsaravjit/llm-gateway/internal/redisx/redistest"
)

// lockedRec is a Recorder that is safe for concurrent use (like metrics.Registry).
type lockedRec struct {
	mu sync.Mutex
	m  map[string]float64
}

func newRec() *lockedRec { return &lockedRec{m: map[string]float64{}} }

func (r *lockedRec) Inc(name string, v float64) { r.mu.Lock(); r.m[name] += v; r.mu.Unlock() }

func (r *lockedRec) get(name string) float64 { r.mu.Lock(); defer r.mu.Unlock(); return r.m[name] }

// slowFetch counts provider calls; each takes d and stores the answer.
func slowFetch(calls *atomic.Int32, c Cache, key string, d time.Duration) FetchFunc {
	return func(ctx context.Context) (*api.ChatResponse, error) {
		calls.Add(1)
		time.Sleep(d)
		resp := &api.ChatResponse{ID: "answer"}
		return resp, c.Set(ctx, key, resp, time.Minute)
	}
}

// burst runs n concurrent Do calls and counts outcomes.
func burst(t *testing.T, co *Coalescer, n int, key string, fetch FetchFunc) map[Outcome]int {
	t.Helper()
	var mu sync.Mutex
	got := map[Outcome]int{}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp, out, err := co.Do(context.Background(), key, fetch)
			if err != nil || resp == nil || resp.ID != "answer" {
				t.Errorf("Do: resp=%v err=%v", resp, err)
			}
			mu.Lock()
			got[out]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	return got
}

func TestCoalesceOffCallsEveryTime(t *testing.T) {
	m := NewMemory()
	co, _ := NewCoalescer(CoalesceOff, m, newRec())
	var calls atomic.Int32
	burst(t, co, 20, "k", slowFetch(&calls, m, "k", 20*time.Millisecond))
	if n := calls.Load(); n != 20 {
		t.Fatalf("provider calls = %d, want 20", n)
	}
}

func TestCoalesceLocal100To1(t *testing.T) {
	m, rec := NewMemory(), newRec()
	co, _ := NewCoalescer(CoalesceLocal, m, rec)
	var calls atomic.Int32
	got := burst(t, co, 100, "k", slowFetch(&calls, m, "k", 100*time.Millisecond))
	if n := calls.Load(); n != 1 {
		t.Fatalf("provider calls = %d, want 1", n)
	}
	if got[Fetched] != 1 || got[SharedLocal] != 99 {
		t.Fatalf("outcomes = %v, want 1 fetched and 99 shared", got)
	}
	if v := rec.get(`cache_coalesced_total{level="local"}`); v != 99 {
		t.Fatalf("coalesced local = %v, want 99", v)
	}
}

func TestCoalesceLocalSharesError(t *testing.T) {
	co, _ := NewCoalescer(CoalesceLocal, NewMemory(), newRec())
	var calls atomic.Int32
	boom := errors.New("provider 502")
	fetch := func(context.Context) (*api.ChatResponse, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return nil, boom
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := co.Do(context.Background(), "k", fetch); !errors.Is(err, boom) {
				t.Errorf("err = %v, want provider error", err)
			}
		}()
	}
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("provider calls = %d, want 1", n)
	}
}

// A waiter that gives up (client gone) does not cancel the shared call.
func TestCoalesceWaiterCancelDoesNotStopLeader(t *testing.T) {
	m := NewMemory()
	co, _ := NewCoalescer(CoalesceLocal, m, newRec())
	var calls atomic.Int32
	fetch := slowFetch(&calls, m, "k", 150*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, _, err := co.Do(ctx, "k", fetch); errc <- err }() // first caller leads
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller err = %v", err)
	}
	resp, _, err := co.Do(context.Background(), "k", fetch) // joins the same call
	if err != nil || resp.ID != "answer" || calls.Load() != 1 {
		t.Fatalf("resp=%v err=%v calls=%d, want the shared answer and 1 call", resp, err, calls.Load())
	}
}

// Two "nodes" share one Redis: a burst on both calls the provider once.
func TestCoalesceRedisAcrossNodes(t *testing.T) {
	_, client := redistest.Start(t)
	shared := NewRedis(client)
	recA, recB := newRec(), newRec()
	nodeA, _ := NewCoalescer(CoalesceRedis, shared, recA, WithRedisLock(client, 5*time.Second), WithWait(2*time.Second, 10*time.Millisecond))
	nodeB, _ := NewCoalescer(CoalesceRedis, shared, recB, WithRedisLock(client, 5*time.Second), WithWait(2*time.Second, 10*time.Millisecond))

	var calls atomic.Int32
	key := Key("cheap", []api.Message{{Role: "user", Content: "hi"}}, 0)
	fetch := slowFetch(&calls, shared, key, 200*time.Millisecond)
	var wg sync.WaitGroup
	results := make([]map[Outcome]int, 2)
	for i, co := range []*Coalescer{nodeA, nodeB} {
		wg.Add(1)
		go func() { defer wg.Done(); results[i] = burst(t, co, 50, key, fetch) }()
	}
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("provider calls = %d, want 1 across both nodes (outcomes %v)", n, results)
	}
	fetched := results[0][Fetched] + results[1][Fetched]
	redis := results[0][SharedRedis] + results[1][SharedRedis]
	if fetched != 1 || redis != 50 {
		t.Fatalf("fetched=%d sharedRedis=%d (outcomes %v), want 1 and 50 (the other node's burst)", fetched, redis, results)
	}
	if v, _ := client.Exists(context.Background(), "lock:"+key).Result(); v != 0 {
		t.Fatal("lock not released")
	}
}

// A lock held by a node that never answers: waiters fall back after the wait.
func TestCoalesceRedisFallbackAfterWait(t *testing.T) {
	_, client := redistest.Start(t)
	shared, rec := NewRedis(client), newRec()
	co, _ := NewCoalescer(CoalesceRedis, shared, rec, WithRedisLock(client, 5*time.Second), WithWait(150*time.Millisecond, 10*time.Millisecond))
	client.Set(context.Background(), "lock:k", "dead-node", 5*time.Second)

	var calls atomic.Int32
	start := time.Now()
	resp, out, err := co.Do(context.Background(), "k", slowFetch(&calls, shared, "k", 0))
	if err != nil || resp.ID != "answer" || out != Fetched || calls.Load() != 1 {
		t.Fatalf("resp=%v out=%v err=%v calls=%d", resp, out, err, calls.Load())
	}
	if el := time.Since(start); el < 150*time.Millisecond {
		t.Fatalf("fell back after %v, want about the 150 ms wait", el)
	}
	if rec.get("cache_coalesce_fallback_total") != 1 {
		t.Fatalf("fallback = %v, want 1", rec.get("cache_coalesce_fallback_total"))
	}
	if v, _ := client.Get(context.Background(), "lock:k").Result(); v != "dead-node" {
		t.Fatalf("someone else's lock was touched: %q", v)
	}
}

func TestCoalesceRedisReleaseOnlyOwnLock(t *testing.T) {
	_, client := redistest.Start(t)
	co, _ := NewCoalescer(CoalesceRedis, NewRedis(client), newRec(), WithRedisLock(client, time.Second))
	ctx := context.Background()
	client.Set(ctx, "lock:k", "other-token", time.Second)
	co.release(ctx, "lock:k", "my-token")
	if v, _ := client.Get(ctx, "lock:k").Result(); v != "other-token" {
		t.Fatalf("lock = %q, want other-token kept", v)
	}
	co.release(ctx, "lock:k", "other-token")
	if v, _ := client.Exists(ctx, "lock:k").Result(); v != 0 {
		t.Fatal("own lock not deleted")
	}
}

// Redis down: skip the lock, still answer (local coalescing still applies).
func TestCoalesceRedisDownFailsOpen(t *testing.T) {
	mr, _ := redistest.Start(t)
	proxy := redistest.NewFaultProxy(t, mr.Addr())
	client := redistest.NewClient(t, proxy.Addr())
	proxy.Down()
	m, rec := NewMemory(), newRec()
	co, _ := NewCoalescer(CoalesceRedis, m, rec, WithRedisLock(client, time.Second))
	var calls atomic.Int32
	got := burst(t, co, 20, "k", slowFetch(&calls, m, "k", 50*time.Millisecond))
	if calls.Load() != 1 || got[Fetched] != 1 {
		t.Fatalf("calls=%d outcomes=%v, want 1 call", calls.Load(), got)
	}
	if rec.get(`cache_errors_total{op="lock"}`) < 1 {
		t.Fatal("lock error not counted")
	}
}

func TestParseCoalesceMode(t *testing.T) {
	for in, want := range map[string]CoalesceMode{"": CoalesceOff, "off": CoalesceOff, "local": CoalesceLocal, "redis": CoalesceRedis} {
		if got, err := ParseCoalesceMode(in); err != nil || got != want {
			t.Errorf("%q -> %v, %v", in, got, err)
		}
	}
	if _, err := ParseCoalesceMode("yes"); err == nil {
		t.Error("want error for bad mode")
	}
	if _, err := NewCoalescer(CoalesceRedis, NewMemory(), newRec()); err == nil {
		t.Error("redis mode without client should fail")
	}
}
