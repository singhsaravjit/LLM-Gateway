package main

import (
	"context"
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
