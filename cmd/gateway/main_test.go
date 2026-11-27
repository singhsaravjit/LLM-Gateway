package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
	"github.com/singhsaravjit/llm-gateway/internal/budget"
	"github.com/singhsaravjit/llm-gateway/internal/cache"
	"github.com/singhsaravjit/llm-gateway/internal/metrics"
	"github.com/singhsaravjit/llm-gateway/internal/provider"
	"github.com/singhsaravjit/llm-gateway/internal/queue"
	"github.com/singhsaravjit/llm-gateway/internal/ratelimit"
	"github.com/singhsaravjit/llm-gateway/internal/router"
)

var testUsage = api.Usage{PromptTokens: 100, CompletionTokens: 200, TotalTokens: 300}

type fakeProvider struct{ calls atomic.Int32 }

func (p *fakeProvider) Complete(_ context.Context, model string, _ *api.ChatRequest) (*api.ChatResponse, error) {
	p.calls.Add(1)
	return &api.ChatResponse{ID: "x", Model: model, Usage: testUsage}, nil
}

func newTestServer(cacheOn bool) (*server, *fakeProvider) {
	reg := metrics.New()
	p := &fakeProvider{}
	return &server{
		limiter:  ratelimit.NewLocal(1000, 1000),
		budget:   budget.NewMemory(budget.Limits{}), // unlimited
		cache:    cache.NewInstrumented(cache.NewMemory(), reg),
		cacheOn:  cacheOn,
		cacheTTL: time.Minute,
		router:   router.LengthRouter{MaxCheapChars: 500},
		provider: p,
		metrics:  reg,
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, p
}

func send(t *testing.T, s *server) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"messages":[{"role":"user","content":"hello"}]}`))
	r.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	s.handleChat(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	return w
}

// metric reads one value from the /metrics output (0 if absent).
func metric(t *testing.T, s *server, name string) float64 {
	t.Helper()
	w := httptest.NewRecorder()
	s.metrics.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if v, ok := strings.CutPrefix(line, "llmgw_"+name+" "); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatal(err)
			}
			return f
		}
	}
	return 0
}

func TestCacheHitRecordsCostSaved(t *testing.T) {
	s, p := newTestServer(true)
	if got := send(t, s).Header().Get(api.HeaderCache); got != "MISS" {
		t.Fatalf("first request X-Cache=%s, want MISS", got)
	}
	w := send(t, s)
	if got := w.Header().Get(api.HeaderCache); got != "HIT" {
		t.Fatalf("second request X-Cache=%s, want HIT", got)
	}
	if got := w.Header().Get(api.HeaderCostUSD); got != "0.000000" {
		t.Errorf("hit X-Cost-USD=%s, want 0", got)
	}
	if n := p.calls.Load(); n != 1 {
		t.Errorf("provider calls = %d, want 1", n)
	}
	want := provider.Cost("cheap", testUsage)
	if got := metric(t, s, `cost_saved_usd_total{model="cheap"}`); got != want {
		t.Errorf("cost_saved_usd_total = %v, want %v", got, want)
	}
	if got := metric(t, s, `cost_usd_total{model="cheap"}`); got != want {
		t.Errorf("cost_usd_total = %v, want %v (only the miss is paid)", got, want)
	}
	if hit, miss := metric(t, s, `cache_lookups_total{result="hit"}`), metric(t, s, `cache_lookups_total{result="miss"}`); hit != 1 || miss != 1 {
		t.Errorf("lookups hit=%v miss=%v, want 1 and 1", hit, miss)
	}
}

func TestCacheDisabledBypasses(t *testing.T) {
	s, p := newTestServer(false)
	for i := 0; i < 2; i++ {
		if got := send(t, s).Header().Get(api.HeaderCache); got != "BYPASS" {
			t.Fatalf("X-Cache=%s, want BYPASS", got)
		}
	}
	if n := p.calls.Load(); n != 2 {
		t.Errorf("provider calls = %d, want 2", n)
	}
	if got := metric(t, s, "cache_lookup_seconds_count"); got != 0 {
		t.Errorf("cache was looked up %v times with CACHE_ENABLED=false", got)
	}
}

// sendPrompt is send without the 200 check, for a given prompt and team.
func sendPrompt(s *server, team, prompt string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"messages":[{"role":"user","content":"`+prompt+`"}]}`))
	r.Header.Set("Authorization", "Bearer test-key")
	r.Header.Set(api.HeaderTeam, team)
	w := httptest.NewRecorder()
	s.handleChat(w, r)
	return w
}

// Over budget: new prompts get 402, but cached answers (free) are still served
// and other teams are unaffected.
func TestBudgetBlocksMissesButServesHits(t *testing.T) {
	s, p := newTestServer(true)
	oneCall := provider.Cost("cheap", testUsage)
	s.budget = budget.NewMemory(budget.Limits{PerTeam: map[string]float64{"small": oneCall}})

	if w := sendPrompt(s, "small", "first"); w.Code != http.StatusOK {
		t.Fatalf("first request: %d %s", w.Code, w.Body.String())
	}
	if w := sendPrompt(s, "small", "second"); w.Code != http.StatusPaymentRequired {
		t.Fatalf("over budget, new prompt: %d, want 402", w.Code)
	}
	w := sendPrompt(s, "small", "first")
	if w.Code != http.StatusOK || w.Header().Get(api.HeaderCache) != "HIT" {
		t.Fatalf("over budget, cached prompt: %d X-Cache=%s, want 200 HIT", w.Code, w.Header().Get(api.HeaderCache))
	}
	if w := sendPrompt(s, "other", "second"); w.Code != http.StatusOK {
		t.Fatalf("other team: %d, want 200", w.Code)
	}
	if n := p.calls.Load(); n != 2 {
		t.Errorf("provider calls = %d, want 2 (the 402 and the hit must not call it)", n)
	}
	if got := metric(t, s, `requests_total{result="budget_exceeded"}`); got != 1 {
		t.Errorf("budget_exceeded = %v, want 1", got)
	}
}

// fakeQueue returns a fixed error from Submit.
type fakeQueue struct{ err error }

func (f fakeQueue) Submit(context.Context, string, *api.ChatRequest) (*api.ChatResponse, error) {
	return nil, f.err
}
func (fakeQueue) Depth(context.Context) (int64, error) { return 0, nil }
func (fakeQueue) Close() error                         { return nil }

type denyPacer struct{}

func (denyPacer) TryAcquire(context.Context, string) bool  { return false }
func (denyPacer) Wait(ctx context.Context, _ string) error { return ctx.Err() }

// With the provider at its limit, requests go to the queue; a full queue is
// 503 queue_full, a queue timeout 504, and a Redis failure falls back to a
// direct provider call.
func TestQueueErrorsMapToStatus(t *testing.T) {
	for _, tc := range []struct {
		err       error
		code      int
		providerN int32
	}{
		{queue.ErrFull, http.StatusServiceUnavailable, 0},
		{queue.ErrTimeout, http.StatusGatewayTimeout, 0},
		{fmt.Errorf("%w: redis down", queue.ErrUnavailable), http.StatusOK, 1},
	} {
		s, p := newTestServer(false)
		s.queue, s.pacer = fakeQueue{tc.err}, denyPacer{}
		if w := sendPrompt(s, "t", "x"); w.Code != tc.code {
			t.Errorf("%v: status %d, want %d (%s)", tc.err, w.Code, tc.code, w.Body.String())
		}
		if n := p.calls.Load(); n != tc.providerN {
			t.Errorf("%v: provider calls = %d, want %d", tc.err, n, tc.providerN)
		}
	}
}

type errProvider struct{ err error }

func (p errProvider) Complete(context.Context, string, *api.ChatRequest) (*api.ChatResponse, error) {
	return nil, p.err
}

// slowProvider never answers; it only returns when the context is done.
type slowProvider struct{}

func (slowProvider) Complete(ctx context.Context, _ string, _ *api.ChatRequest) (*api.ChatResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestBadRequests(t *testing.T) {
	s, p := newTestServer(true)
	for _, tc := range []struct {
		name, auth, body string
		code             int
	}{
		{"missing key", "", `{"messages":[{"role":"user","content":"hi"}]}`, http.StatusUnauthorized},
		{"invalid json", "Bearer k", `{"messages":`, http.StatusBadRequest},
		{"no messages", "Bearer k", `{"messages":[]}`, http.StatusBadRequest},
	} {
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(tc.body))
		if tc.auth != "" {
			r.Header.Set(api.HeaderAPIKey, tc.auth)
		}
		w := httptest.NewRecorder()
		s.handleChat(w, r)
		if w.Code != tc.code {
			t.Errorf("%s: status %d, want %d", tc.name, w.Code, tc.code)
		}
		if !strings.Contains(w.Body.String(), `"bad_request"`) {
			t.Errorf("%s: body %s, want a bad_request error", tc.name, w.Body.String())
		}
	}
	if n := p.calls.Load(); n != 0 {
		t.Errorf("provider calls = %d, want 0", n)
	}
}

// Upstream 429 is passed through as 429; any other provider failure is 502.
// A failed request must not be cached.
func TestUpstreamErrorsMapToStatus(t *testing.T) {
	for _, tc := range []struct {
		upstream, want int
	}{
		{http.StatusTooManyRequests, http.StatusTooManyRequests},
		{http.StatusInternalServerError, http.StatusBadGateway},
	} {
		s, _ := newTestServer(true)
		s.provider = errProvider{&provider.StatusError{Code: tc.upstream, Body: "boom"}}
		w := sendPrompt(s, "t", "x")
		if w.Code != tc.want {
			t.Errorf("upstream %d: status %d, want %d", tc.upstream, w.Code, tc.want)
		}
		if !strings.Contains(w.Body.String(), `"upstream_error"`) {
			t.Errorf("upstream %d: body %s, want an upstream_error", tc.upstream, w.Body.String())
		}
		ok := &fakeProvider{}
		s.provider = ok
		if w := sendPrompt(s, "t", "x"); w.Header().Get(api.HeaderCache) != "MISS" || ok.calls.Load() != 1 {
			t.Errorf("upstream %d: the failed response was cached", tc.upstream)
		}
	}
}

func TestProviderCallTimesOut(t *testing.T) {
	s, _ := newTestServer(false)
	s.provider = slowProvider{}
	s.reqTimeout = 50 * time.Millisecond
	start := time.Now()
	w := sendPrompt(s, "t", "x")
	if w.Code != http.StatusBadGateway {
		t.Errorf("status %d, want 502", w.Code)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("request took %v, the timeout did not fire", d)
	}
}
