package queue

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/singhsaravjit/llm-gateway/internal/api"
	"github.com/singhsaravjit/llm-gateway/internal/provider"
	"github.com/singhsaravjit/llm-gateway/internal/ratelimit"
	"github.com/singhsaravjit/llm-gateway/internal/redisx/redistest"
)

var req = &api.ChatRequest{Messages: []api.Message{{Role: "user", Content: "hi"}}}

func okExec(calls *atomic.Int64) Executor {
	return func(_ context.Context, model string, r *api.ChatRequest) (*api.ChatResponse, error) {
		calls.Add(1)
		return &api.ChatResponse{ID: "r", Model: model, Choices: []api.Choice{{Message: r.Messages[0]}}}, nil
	}
}

// impls builds the same queue twice: in memory, and on Redis with workers on this node.
func impls(t *testing.T, cfg Config) map[string]Queue {
	t.Helper()
	_, c := redistest.Start(t)
	m := NewMemory(cfg)
	r, err := NewRedis(context.Background(), c, RedisConfig{Config: cfg, NodeID: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close(); r.Close() })
	return map[string]Queue{"memory": m, "redis": r}
}

func TestSubmitReturnsProviderAnswer(t *testing.T) {
	var calls atomic.Int64
	for name, q := range impls(t, Config{Workers: 2, Exec: okExec(&calls)}) {
		t.Run(name, func(t *testing.T) {
			resp, err := q.Submit(context.Background(), "cheap", req)
			if err != nil || resp.Model != "cheap" || resp.Choices[0].Message.Content != "hi" {
				t.Fatalf("resp=%+v err=%v", resp, err)
			}
			waitDepth(t, q, 0)
		})
	}
}

func TestPacerHoldsProviderRate(t *testing.T) {
	const rps, jobs = 40, 30
	var mu sync.Mutex
	var times []time.Time
	exec := func(context.Context, string, *api.ChatRequest) (*api.ChatResponse, error) {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		return &api.ChatResponse{}, nil
	}
	for name, q := range impls(t, Config{Workers: 8, Exec: exec,
		Pacer: LimiterPacer{Limiter: ratelimit.NewLocal(rps*60, 1)}}) {
		t.Run(name, func(t *testing.T) {
			times = nil
			start := time.Now()
			var wg sync.WaitGroup
			for i := 0; i < jobs; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _, _ = q.Submit(context.Background(), "cheap", req) }()
			}
			wg.Wait()
			// burst 1 => the 30 calls need at least 29/40 s.
			if el, min := time.Since(start), time.Duration(float64(jobs-1)/rps*0.9*float64(time.Second)); el < min {
				t.Fatalf("%d calls took %v, pacer should need >= %v", jobs, el, min)
			}
			if len(times) != jobs {
				t.Fatalf("calls = %d, want %d", len(times), jobs)
			}
		})
	}
}

func TestFullQueueRejects(t *testing.T) {
	release := make(chan struct{})
	exec := func(ctx context.Context, _ string, _ *api.ChatRequest) (*api.ChatResponse, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return &api.ChatResponse{}, nil
	}
	for name, q := range impls(t, Config{Max: 2, Workers: 1, Exec: exec}) {
		t.Run(name, func(t *testing.T) {
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _, _ = q.Submit(context.Background(), "cheap", req) }()
			}
			waitDepth(t, q, 2)
			if _, err := q.Submit(context.Background(), "cheap", req); !errors.Is(err, ErrFull) {
				t.Fatalf("3rd submit: err=%v, want ErrFull", err)
			}
			release <- struct{}{}
			release <- struct{}{}
			wg.Wait()
		})
	}
}

func TestWaitTimeoutAndExpiredJobsSkipped(t *testing.T) {
	var calls atomic.Int64
	slowPacer := LimiterPacer{Limiter: ratelimit.NewLocal(1, 1), PerModel: true} // 1/min per model: only the first job gets through
	for name, q := range impls(t, Config{Workers: 1, WaitTimeout: 150 * time.Millisecond, Exec: okExec(&calls), Pacer: slowPacer}) {
		t.Run(name, func(t *testing.T) {
			calls.Store(0)
			_, _ = q.Submit(context.Background(), "cheap-"+name, req) // first call uses the only token
			start := time.Now()
			_, err := q.Submit(context.Background(), "cheap-"+name, req)
			if !errors.Is(err, ErrTimeout) || time.Since(start) > time.Second {
				t.Fatalf("err=%v after %v, want ErrTimeout after ~150ms", err, time.Since(start))
			}
			waitDepth(t, q, 0)
			if calls.Load() != 1 {
				t.Fatalf("provider calls = %d; the timed-out job must not be sent", calls.Load())
			}
		})
	}
}

func TestRetriesUpstream429UntilSuccess(t *testing.T) {
	var n atomic.Int64
	exec := func(context.Context, string, *api.ChatRequest) (*api.ChatResponse, error) {
		if n.Add(1)%3 != 0 { // 2 x 429, then success
			return nil, &provider.StatusError{Code: http.StatusTooManyRequests, Body: "slow down"}
		}
		return &api.ChatResponse{ID: "ok"}, nil
	}
	for name, q := range impls(t, Config{Workers: 1, Exec: exec}) {
		t.Run(name, func(t *testing.T) {
			resp, err := q.Submit(context.Background(), "cheap", req)
			if err != nil || resp.ID != "ok" {
				t.Fatalf("resp=%+v err=%v", resp, err)
			}
		})
	}
}

func TestProviderErrorIsPassedBack(t *testing.T) {
	exec := func(context.Context, string, *api.ChatRequest) (*api.ChatResponse, error) {
		return nil, &provider.StatusError{Code: 500, Body: "boom"}
	}
	for name, q := range impls(t, Config{Workers: 1, Exec: exec}) {
		t.Run(name, func(t *testing.T) {
			_, err := q.Submit(context.Background(), "cheap", req)
			var se *provider.StatusError
			if !errors.As(err, &se) || se.Code != 500 {
				t.Fatalf("err=%v, want provider 500", err)
			}
		})
	}
}

// --- Redis only ---

// A request submitted on node A is executed by a worker on node B and the
// answer comes back to A.
func TestRedisCrossNode(t *testing.T) {
	_, c := redistest.Start(t)
	var calls atomic.Int64
	a, err := NewRedis(context.Background(), c, RedisConfig{Config: Config{Workers: 0, Exec: okExec(&calls)}, NodeID: "A"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewRedis(context.Background(), c, RedisConfig{Config: Config{Workers: 2, Exec: okExec(&calls)}, NodeID: "B"})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	defer b.Close()
	var wg sync.WaitGroup
	var fails atomic.Int64
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := &api.ChatRequest{Messages: []api.Message{{Role: "user", Content: fmt.Sprint(i)}}}
			resp, err := a.Submit(context.Background(), "cheap", r)
			if err != nil || resp.Choices[0].Message.Content != fmt.Sprint(i) {
				fails.Add(1) // wrong answer = replies matched to the wrong request
			}
		}(i)
	}
	wg.Wait()
	if fails.Load() != 0 || calls.Load() != 20 {
		t.Fatalf("fails=%d calls=%d", fails.Load(), calls.Load())
	}
}

// A worker that takes a job and dies never acks it. Visibility must exceed
// the wait timeout (so a slow live job is never run twice), which means the
// client has given up by the time the job is reclaimed. Reclaiming therefore
// cleans up: the orphan leaves the queue (depth stays accurate) and the
// provider is never called for it.
func TestRedisReclaimsJobsFromDeadWorker(t *testing.T) {
	_, c := redistest.Start(t)
	var calls atomic.Int64
	cfg := Config{WaitTimeout: 300 * time.Millisecond, Exec: okExec(&calls)}
	a, err := NewRedis(context.Background(), c, RedisConfig{Config: cfg, NodeID: "A"}) // submits only
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	got := make(chan error, 1)
	go func() { _, err := a.Submit(context.Background(), "cheap", req); got <- err }()

	// The "dead" worker reads the job and never acknowledges it.
	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Second)
	for {
		res, _ := c.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "dead",
			Streams: []string{"queue:provider", ">"}, Count: 1, Block: 100 * time.Millisecond}).Result()
		if len(res) > 0 && len(res[0].Messages) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job never appeared")
		}
	}
	if err := <-got; !errors.Is(err, ErrTimeout) {
		t.Fatalf("client: err=%v, want ErrTimeout", err)
	}
	if n, _ := a.Depth(ctx); n != 1 {
		t.Fatalf("orphaned job should still be in the stream, depth=%d", n)
	}

	cfg.Workers = 1
	b, err := NewRedis(context.Background(), c, RedisConfig{Config: cfg, NodeID: "B", Visibility: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	waitDepth(t, b, 0)
	if calls.Load() != 0 {
		t.Fatalf("provider called %d times for an abandoned job", calls.Load())
	}
}

func TestRedisVisibilityMustExceedWait(t *testing.T) {
	_, c := redistest.Start(t)
	_, err := NewRedis(context.Background(), c, RedisConfig{Config: Config{WaitTimeout: time.Minute}, NodeID: "x", Visibility: time.Second})
	if err == nil {
		t.Fatal("expected error")
	}
}

func waitDepth(t *testing.T, q Queue, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		n, err := q.Depth(context.Background())
		if err == nil && n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("depth = %d (err %v), want %d", n, err, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRedisDownIsUnavailable(t *testing.T) {
	mr, _ := redistest.Start(t)
	p := redistest.NewFaultProxy(t, mr.Addr())
	c := redistest.NewClient(t, p.Addr())
	q, err := NewRedis(context.Background(), c, RedisConfig{Config: Config{Workers: 1, Exec: okExec(new(atomic.Int64))}, NodeID: "n"})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	p.Down()
	start := time.Now()
	if _, err := q.Submit(context.Background(), "cheap", req); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v, want ErrUnavailable", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("Submit took %v with Redis down; should fail fast", time.Since(start))
	}
}

// A provider call cut off by the job's deadline is a timeout (504), not a
// provider error (502).
func TestDeadlineDuringProviderCallIsTimeout(t *testing.T) {
	exec := func(ctx context.Context, _ string, _ *api.ChatRequest) (*api.ChatResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	for name, q := range impls(t, Config{Workers: 1, WaitTimeout: 100 * time.Millisecond, Exec: exec}) {
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 5; i++ {
				if _, err := q.Submit(context.Background(), "cheap", req); !errors.Is(err, ErrTimeout) {
					t.Fatalf("err=%v, want ErrTimeout", err)
				}
			}
		})
	}
}

// Redis down when the gateway starts must not stop it from starting; the
// queue works as soon as Redis is back.
func TestRedisDownAtStartupRecovers(t *testing.T) {
	mr, _ := redistest.Start(t)
	p := redistest.NewFaultProxy(t, mr.Addr())
	c := redistest.NewClient(t, p.Addr())
	p.Down()
	var calls atomic.Int64
	q, err := NewRedis(context.Background(), c, RedisConfig{Config: Config{Workers: 1, Exec: okExec(&calls)}, NodeID: "n"})
	if err != nil {
		t.Fatalf("NewRedis with Redis down: %v", err)
	}
	defer q.Close()
	if _, err := q.Submit(context.Background(), "cheap", req); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v, want ErrUnavailable", err)
	}
	p.Up()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := q.Submit(ctx, "cheap", req)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue never recovered: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
