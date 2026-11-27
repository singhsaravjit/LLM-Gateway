package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
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

type fakeProvider struct {
	calls atomic.Int32
	delay time.Duration
}

func (p *fakeProvider) Complete(_ context.Context, model string, _ *api.ChatRequest) (*api.ChatResponse, error) {
	p.calls.Add(1)
	time.Sleep(p.delay)
	return &api.ChatResponse{ID: "x", Model: model, Usage: testUsage}, nil
}

func newTestServer(cacheOn bool, mode cache.CoalesceMode) (*server, *fakeProvider) {
	reg := metrics.New()
	p := &fakeProvider{}
	mem := cache.NewMemory()
	co, _ := cache.NewCoalescer(mode, mem, reg)
	return &server{
		limiter:  ratelimit.NewLocal(1000, 1000),
		budget:   budget.NewMemory(budget.Limits{}), // unlimited
		cache:    cache.NewInstrumented(mem, reg),
		cacheOn:  cacheOn,
		cacheTTL: time.Minute,
		coalesce: co,
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
	if w.Code != http.StatusOK { // Errorf, not Fatalf: send also runs in goroutines
		t.Errorf("status %d: %s", w.Code, w.Body.String())
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
	s, p := newTestServer(true, cache.CoalesceOff)
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
	s, p := newTestServer(false, cache.CoalesceOff)
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
	s, p := newTestServer(true, cache.CoalesceOff)
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
		s, p := newTestServer(false, cache.CoalesceOff)
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
	s, p := newTestServer(true, cache.CoalesceOff)
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
		s, _ := newTestServer(true, cache.CoalesceOff)
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

// downProvider answers 500 for the models in down and works for the rest.
type downProvider struct {
	down  map[string]bool
	calls atomic.Int32
	delay time.Duration
}

func (p *downProvider) Complete(_ context.Context, model string, _ *api.ChatRequest) (*api.ChatResponse, error) {
	p.calls.Add(1)
	time.Sleep(p.delay)
	if p.down[model] {
		return nil, &provider.StatusError{Code: 500, Body: "down"}
	}
	return &api.ChatResponse{ID: "x", Model: "real-" + model, Usage: testUsage}, nil
}

func resilientFor(s *server, down ...string) *downProvider {
	p := &downProvider{down: map[string]bool{}}
	for _, m := range down {
		p.down[m] = true
	}
	cfg := provider.DefaultResilientConfig()
	cfg.BaseBackoff, cfg.MaxBackoff = time.Millisecond, time.Millisecond
	s.provider = provider.NewResilient(p, cfg, s.metrics)
	return p
}

func sendModel(s *server, model string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"`+model+`","messages":[{"role":"user","content":"hello"}]}`))
	r.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	s.handleChat(w, r)
	return w
}

// strong is down: the request is served by cheap, reported and charged as
// cheap, and not cached under strong's key (or at all).
func TestFallbackReportsAndChargesActualModel(t *testing.T) {
	s, _ := newTestServer(true, cache.CoalesceOff)
	p := resilientFor(s, "strong")
	w := sendModel(s, "strong")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get(api.HeaderModel); got != "cheap" {
		t.Errorf("X-Model-Used=%s, want cheap", got)
	}
	want := provider.Cost("cheap", testUsage)
	if got := w.Header().Get(api.HeaderCostUSD); got != strconv.FormatFloat(want, 'f', 6, 64) {
		t.Errorf("X-Cost-USD=%s, want %f (cheap price)", got, want)
	}
	if got := metric(t, s, `cost_usd_total{model="cheap"}`); got != want {
		t.Errorf("cost_usd_total{cheap} = %v, want %v", got, want)
	}
	if got := metric(t, s, `cost_usd_total{model="strong"}`); got != 0 {
		t.Errorf("cost_usd_total{strong} = %v, want 0", got)
	}
	if got := metric(t, s, `provider_fallbacks_total{from="strong",to="cheap"}`); got != 1 {
		t.Errorf("fallbacks = %v, want 1", got)
	}
	msgs := []api.Message{{Role: "user", Content: "hello"}}
	for _, m := range []string{"strong", "cheap"} {
		if _, hit, _ := s.cache.Get(context.Background(), cache.Key(m, msgs)); hit {
			t.Errorf("fallback response was cached under %s", m)
		}
	}
	// strong is back: the same request must reach the provider, not a cache entry
	p.down = nil
	calls := p.calls.Load()
	w = sendModel(s, "strong")
	if w.Header().Get(api.HeaderCache) != "MISS" || w.Header().Get(api.HeaderModel) != "strong" || p.calls.Load() != calls+1 {
		t.Errorf("after recovery: X-Cache=%s X-Model-Used=%s, want MISS strong from the provider",
			w.Header().Get(api.HeaderCache), w.Header().Get(api.HeaderModel))
	}
}

// Coalesced waiters share the leader's fallback answer: all report cheap, only
// the leader pays (at cheap's price), and nothing is cached.
func TestCoalescedFallback(t *testing.T) {
	s, _ := newTestServer(true, cache.CoalesceLocal)
	p := resilientFor(s, "strong")
	p.delay = 50 * time.Millisecond
	var wg sync.WaitGroup
	var mu sync.Mutex
	got := map[string]int{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := sendModel(s, "strong")
			mu.Lock()
			got[w.Header().Get(api.HeaderCache)+" "+w.Header().Get(api.HeaderModel)]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if got["MISS cheap"] != 1 || got["COALESCED cheap"] != 19 {
		t.Fatalf("X-Cache/X-Model-Used counts %v, want 1 MISS cheap and 19 COALESCED cheap", got)
	}
	cost := provider.Cost("cheap", testUsage)
	if v := metric(t, s, `cost_usd_total{model="cheap"}`); v != cost {
		t.Errorf("cost paid = %v, want one cheap call (%v)", v, cost)
	}
	if v := metric(t, s, `cost_saved_usd_total{model="cheap"}`); math.Abs(v-19*cost) > 1e-12 {
		t.Errorf("cost saved = %v, want %v at cheap's price", v, 19*cost)
	}
	if v := metric(t, s, `requests_total{result="ok",cache="coalesced",model="strong"}`); v != 0 {
		t.Errorf("%v coalesced requests reported as strong", v)
	}
	msgs := []api.Message{{Role: "user", Content: "hello"}}
	if _, hit, _ := s.cache.Get(context.Background(), cache.Key("strong", msgs)); hit {
		t.Error("fallback response was cached under strong")
	}
}

func TestBothModelsDownIs502(t *testing.T) {
	s, _ := newTestServer(true, cache.CoalesceOff)
	resilientFor(s, "strong", "cheap")
	w := sendModel(s, "strong")
	if w.Code != http.StatusBadGateway || !strings.Contains(w.Body.String(), `"upstream_error"`) {
		t.Errorf("status %d %s, want 502 upstream_error", w.Code, w.Body.String())
	}
}

func TestProviderCallTimesOut(t *testing.T) {
	s, _ := newTestServer(false, cache.CoalesceOff)
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

// The #13 acceptance test: 100 concurrent identical prompts, 1 provider call.
func TestCoalesce100ConcurrentTo1(t *testing.T) {
	s, p := newTestServer(true, cache.CoalesceLocal)
	p.delay = 200 * time.Millisecond
	var wg sync.WaitGroup
	var mu sync.Mutex
	got := map[string]int{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x := send(t, s).Header().Get(api.HeaderCache)
			mu.Lock()
			got[x]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if n := p.calls.Load(); n != 1 {
		t.Fatalf("provider calls = %d, want 1 (X-Cache counts %v)", n, got)
	}
	if got["MISS"] != 1 || got["COALESCED"]+got["HIT"] != 99 {
		t.Fatalf("X-Cache counts %v, want 1 MISS and 99 COALESCED or HIT", got)
	}
	cost := provider.Cost("cheap", testUsage)
	if v := metric(t, s, `cost_usd_total{model="cheap"}`); v != cost {
		t.Errorf("cost paid = %v, want one call (%v)", v, cost)
	}
	if v := metric(t, s, `cache_coalesced_total{level="local"}`); int(v) != got["COALESCED"] {
		t.Errorf("coalesced metric = %v, want %d", v, got["COALESCED"])
	}
}

func TestCoalesceOffEveryMissCalls(t *testing.T) {
	s, p := newTestServer(true, cache.CoalesceOff)
	p.delay = 100 * time.Millisecond
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); send(t, s) }()
	}
	wg.Wait()
	if n := p.calls.Load(); n != 20 {
		t.Fatalf("provider calls = %d, want 20 with coalescing off", n)
	}
}

// Coalescing sits in front of the queue: if the one shared call is rejected
// by the queue, every waiter gets the same status, and no one calls the provider.
func TestCoalesceSharesQueueError(t *testing.T) {
	s, p := newTestServer(true, cache.CoalesceLocal)
	s.queue, s.pacer = slowQueue{err: queue.ErrFull, delay: 100 * time.Millisecond}, denyPacer{}
	var wg sync.WaitGroup
	codes := make([]int, 20)
	for i := range codes {
		wg.Add(1)
		go func() { defer wg.Done(); codes[i] = sendPrompt(s, "t", "same").Code }()
	}
	wg.Wait()
	for i, c := range codes {
		if c != http.StatusServiceUnavailable {
			t.Errorf("request %d: status %d, want 503 queue_full", i, c)
		}
	}
	if n := p.calls.Load(); n != 0 {
		t.Errorf("provider calls = %d, want 0", n)
	}
}

// slowQueue is fakeQueue with a delay, so concurrent requests overlap.
type slowQueue struct {
	err   error
	delay time.Duration
}

func (q slowQueue) Submit(context.Context, string, *api.ChatRequest) (*api.ChatResponse, error) {
	time.Sleep(q.delay)
	return nil, q.err
}
func (slowQueue) Depth(context.Context) (int64, error) { return 0, nil }
func (slowQueue) Close() error                         { return nil }

// A reply cached at a small max_tokens must not answer a request that allows more.
func TestCacheKeyIncludesMaxTokens(t *testing.T) {
	s, p := newTestServer(true, cache.CoalesceOff)
	sendMax := func(n int) string {
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
			strings.NewReader(`{"messages":[{"role":"user","content":"hello"}],"max_tokens":`+strconv.Itoa(n)+`}`))
		r.Header.Set("Authorization", "Bearer test-key")
		w := httptest.NewRecorder()
		s.handleChat(w, r)
		return w.Header().Get(api.HeaderCache)
	}
	got := []string{sendMax(50), sendMax(500), sendMax(50)}
	if want := []string{"MISS", "MISS", "HIT"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("X-Cache = %v, want %v", got, want)
	}
	if n := p.calls.Load(); n != 2 {
		t.Errorf("provider calls = %d, want 2", n)
	}
}
