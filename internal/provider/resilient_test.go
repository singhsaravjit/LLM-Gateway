package provider

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
	"github.com/singhsaravjit/llm-gateway/internal/metrics"
)

// fakeClient fails on demand: fail gets the model and the call number for
// that model (starting at 1) and returns the error to give, or nil.
type fakeClient struct {
	fail  func(model string, n int) error
	mu    sync.Mutex
	calls map[string]int
}

func (f *fakeClient) Complete(ctx context.Context, model string, _ *api.ChatRequest) (*api.ChatResponse, error) {
	f.mu.Lock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[model]++
	n := f.calls[model]
	f.mu.Unlock()
	if f.fail != nil {
		if err := f.fail(model, n); err != nil {
			return nil, err
		}
	}
	return &api.ChatResponse{ID: "x", Model: "real-" + model}, nil
}

func (f *fakeClient) n(model string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[model]
}

func status(code int) error { return &StatusError{Code: code, Body: "boom"} }

func testConfig() ResilientConfig {
	c := DefaultResilientConfig()
	c.BaseBackoff, c.MaxBackoff = time.Millisecond, 2*time.Millisecond
	return c
}

func metric(reg *metrics.Registry, name string) float64 {
	w := httptest.NewRecorder()
	reg.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if v, ok := strings.CutPrefix(line, "llmgw_"+name+" "); ok {
			f, _ := strconv.ParseFloat(v, 64)
			return f
		}
	}
	return 0
}

var req = &api.ChatRequest{Messages: []api.Message{{Role: "user", Content: "hi"}}}

func TestRetriesThenSucceeds(t *testing.T) {
	f := &fakeClient{fail: func(_ string, n int) error {
		if n <= 2 {
			return status(500)
		}
		return nil
	}}
	reg := metrics.New()
	resp, err := NewResilient(f, testConfig(), reg).Complete(context.Background(), "strong", req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Model != "strong" || f.n("strong") != 3 || f.n("cheap") != 0 {
		t.Errorf("model=%s strong calls=%d cheap calls=%d, want strong 3 0", resp.Model, f.n("strong"), f.n("cheap"))
	}
	if got := metric(reg, `provider_retries_total{model="strong"}`); got != 2 {
		t.Errorf("retries = %v, want 2", got)
	}
}

func TestConnectionErrorIsRetried(t *testing.T) {
	f := &fakeClient{fail: func(_ string, n int) error {
		if n == 1 {
			return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
		}
		return nil
	}}
	if _, err := NewResilient(f, testConfig(), nil).Complete(context.Background(), "cheap", req); err != nil || f.n("cheap") != 2 {
		t.Errorf("err=%v calls=%d, want nil 2", err, f.n("cheap"))
	}
}

// 4xx and 429 are not retried and do not fall back; the 429 comes back
// unchanged so the overflow queue can handle it.
func TestNoRetryOn4xxOr429(t *testing.T) {
	for _, code := range []int{400, 429} {
		f := &fakeClient{fail: func(string, int) error { return status(code) }}
		r := NewResilient(f, testConfig(), nil)
		_, err := r.Complete(context.Background(), "strong", req)
		var se *StatusError
		if !errors.As(err, &se) || se.Code != code {
			t.Errorf("%d: err = %v, want the StatusError back", code, err)
		}
		if f.n("strong") != 1 || f.n("cheap") != 0 {
			t.Errorf("%d: strong calls=%d cheap calls=%d, want 1 0", code, f.n("strong"), f.n("cheap"))
		}
		if s := r.breaker("strong").State(); s != Closed {
			t.Errorf("%d: breaker %v, want closed", code, s)
		}
	}
}

func TestFallbackAfterRetriesExhausted(t *testing.T) {
	f := &fakeClient{fail: func(m string, _ int) error {
		if m == "strong" {
			return status(503)
		}
		return nil
	}}
	reg := metrics.New()
	resp, err := NewResilient(f, testConfig(), reg).Complete(context.Background(), "strong", req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Model != "cheap" || f.n("strong") != 3 || f.n("cheap") != 1 {
		t.Errorf("model=%s strong=%d cheap=%d, want cheap 3 1", resp.Model, f.n("strong"), f.n("cheap"))
	}
	if got := metric(reg, `provider_fallbacks_total{from="strong",to="cheap"}`); got != 1 {
		t.Errorf("fallbacks = %v, want 1", got)
	}
}

func TestFallbackDisabled(t *testing.T) {
	f := &fakeClient{fail: func(m string, _ int) error { return status(500) }}
	cfg := testConfig()
	cfg.Fallback = false
	if _, err := NewResilient(f, cfg, nil).Complete(context.Background(), "cheap", req); err == nil || f.n("strong") != 0 {
		t.Errorf("err=%v strong calls=%d, want an error and no fallback", err, f.n("strong"))
	}
}

func TestBothModelsFail(t *testing.T) {
	f := &fakeClient{fail: func(string, int) error { return status(500) }}
	_, err := NewResilient(f, testConfig(), nil).Complete(context.Background(), "cheap", req)
	var se *StatusError
	if !errors.As(err, &se) || se.Code != 500 {
		t.Errorf("err = %v, want a 500 StatusError", err)
	}
	if f.n("cheap") != 3 || f.n("strong") != 3 {
		t.Errorf("cheap=%d strong=%d, want 3 3", f.n("cheap"), f.n("strong"))
	}
}

func TestOtherModelsHaveNoFallback(t *testing.T) {
	f := &fakeClient{fail: func(string, int) error { return status(502) }}
	if _, err := NewResilient(f, testConfig(), nil).Complete(context.Background(), "gpt-x", req); err == nil {
		t.Fatal("want an error")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) != 1 || f.calls["gpt-x"] != 3 {
		t.Errorf("calls = %v, want only gpt-x x3", f.calls)
	}
}

// Once strong's breaker is open, requests go straight to cheap without
// calling strong at all.
func TestOpenBreakerFallsBackWithoutCalling(t *testing.T) {
	f := &fakeClient{fail: func(m string, _ int) error {
		if m == "strong" {
			return status(500)
		}
		return nil
	}}
	cfg := testConfig()
	cfg.MaxAttempts, cfg.BreakerFailures = 1, 2
	reg := metrics.New()
	r := NewResilient(f, cfg, reg)
	for i := 0; i < 3; i++ {
		resp, err := r.Complete(context.Background(), "strong", req)
		if err != nil || resp.Model != "cheap" {
			t.Fatalf("request %d: %v %v, want served by cheap", i, resp, err)
		}
	}
	if f.n("strong") != 2 {
		t.Errorf("strong calls = %d, want 2 (third request skipped by the open breaker)", f.n("strong"))
	}
	if got := metric(reg, `breaker_state{model="strong"}`); got != float64(Open) {
		t.Errorf("breaker_state = %v, want %d", got, Open)
	}
}

func TestBothBreakersOpen(t *testing.T) {
	f := &fakeClient{fail: func(string, int) error { return status(500) }}
	cfg := testConfig()
	cfg.MaxAttempts, cfg.BreakerFailures = 1, 1
	r := NewResilient(f, cfg, nil)
	_, _ = r.Complete(context.Background(), "cheap", req) // opens both
	_, err := r.Complete(context.Background(), "cheap", req)
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("err = %v, want ErrCircuitOpen", err)
	}
	if f.n("cheap") != 1 || f.n("strong") != 1 {
		t.Errorf("cheap=%d strong=%d, want 1 1", f.n("cheap"), f.n("strong"))
	}
}

func TestNoSleepPastDeadline(t *testing.T) {
	f := &fakeClient{fail: func(string, int) error { return status(500) }}
	cfg := testConfig()
	cfg.BaseBackoff, cfg.MaxBackoff, cfg.Fallback = 10*time.Second, 10*time.Second, false
	r := NewResilient(f, cfg, nil)
	// full jitter can pick a tiny wait, so try a few times; none may take long
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		start := time.Now()
		_, err := r.Complete(ctx, "gpt-x", req)
		cancel()
		if err == nil || time.Since(start) > time.Second {
			t.Fatalf("err=%v took %v, want an error well before the backoff", err, time.Since(start))
		}
	}
}

func TestCallerCancelIsNotAFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeClient{fail: func(string, int) error { cancel(); return context.Canceled }}
	cfg := testConfig()
	cfg.BreakerFailures = 1
	r := NewResilient(f, cfg, nil)
	if _, err := r.Complete(ctx, "strong", req); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if f.n("strong") != 1 || f.n("cheap") != 0 || r.breaker("strong").State() != Closed {
		t.Errorf("strong=%d cheap=%d breaker=%v, want 1 0 closed", f.n("strong"), f.n("cheap"), r.breaker("strong").State())
	}
}

func TestBreakerStates(t *testing.T) {
	now := time.Unix(0, 0)
	b := &Breaker{Threshold: 3, OpenFor: 10 * time.Second, now: func() time.Time { return now }}
	for i := 0; i < 2; i++ {
		b.Failure()
	}
	b.Success() // resets the count: failures must be consecutive
	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatal("closed breaker rejected a call")
		}
		b.Failure()
	}
	if b.State() != Open || b.Allow() {
		t.Fatalf("after 3 failures: %v, want open and rejecting", b.State())
	}
	now = now.Add(10 * time.Second)
	if !b.Allow() || b.State() != HalfOpen {
		t.Fatalf("after OpenFor: %v, want one probe let through", b.State())
	}
	if b.Allow() {
		t.Fatal("half-open let a second call through")
	}
	b.Failure() // probe failed
	if b.State() != Open || b.Allow() {
		t.Fatalf("after failed probe: %v, want open", b.State())
	}
	now = now.Add(10 * time.Second)
	b.Allow()
	b.Release() // probe got a 4xx: tells us nothing, next caller probes
	if !b.Allow() {
		t.Fatal("after a released probe the next call should probe")
	}
	b.Success()
	if b.State() != Closed || !b.Allow() {
		t.Fatalf("after good probe: %v, want closed", b.State())
	}
}

func TestHalfOpenLetsOneProbeConcurrently(t *testing.T) {
	now := time.Unix(0, 0)
	b := &Breaker{Threshold: 1, OpenFor: time.Second, now: func() time.Time { return now }}
	b.Failure()
	now = now.Add(time.Second)
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Allow() {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := allowed.Load(); n != 1 {
		t.Errorf("%d probes let through, want 1", n)
	}
}
