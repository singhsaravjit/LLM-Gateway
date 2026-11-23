// Command gateway is the LLM request gateway (owner of the HTTP core: Preshit).
//
// Request path: rate limit -> router -> cache -> budget check -> provider.
// Cache hits cost nothing, so they are served even when a team is over budget.
// Everything below uses in-memory implementations so it runs with zero setup;
// each owner swaps in the Redis-backed version of their component.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
	"github.com/singhsaravjit/llm-gateway/internal/budget"
	"github.com/singhsaravjit/llm-gateway/internal/cache"
	"github.com/singhsaravjit/llm-gateway/internal/metrics"
	"github.com/singhsaravjit/llm-gateway/internal/provider"
	"github.com/singhsaravjit/llm-gateway/internal/ratelimit"
	"github.com/singhsaravjit/llm-gateway/internal/redisx"
	"github.com/singhsaravjit/llm-gateway/internal/router"

	"github.com/redis/go-redis/v9"
)

type server struct {
	limiter  ratelimit.Limiter
	budget   budget.Tracker
	cache    cache.Cache
	cacheTTL time.Duration
	router   router.Router
	provider provider.Client
	metrics  *metrics.Registry
	log      *slog.Logger
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	// One Redis client shared by every Redis-backed component (nil without REDIS_ADDR).
	rdb, err := newRedis(log)
	if err != nil {
		log.Error("redis config", "err", err)
		os.Exit(1)
	}
	limiter, err := newLimiter(log, rdb)
	if err != nil {
		log.Error("rate limiter config", "err", err)
		os.Exit(1)
	}
	respCache, err := newCache(log, rdb)
	if err != nil {
		log.Error("cache config", "err", err)
		os.Exit(1)
	}
	budgets, err := newBudget(log, rdb)
	if err != nil {
		log.Error("budget config", "err", err)
		os.Exit(1)
	}
	s := &server{
		limiter:  limiter,
		budget:   budgets,
		cache:    respCache,
		cacheTTL: time.Duration(envInt("CACHE_TTL_SECONDS", 300)) * time.Second,
		router:   router.LengthRouter{MaxCheapChars: envInt("ROUTER_MAX_CHEAP_CHARS", 500)},
		provider: provider.NewHTTP(env("PROVIDER_URL", "http://localhost:9000"), os.Getenv("PROVIDER_API_KEY"),
			time.Duration(envInt("PROVIDER_TIMEOUT_MS", 30000))*time.Millisecond),
		metrics: metrics.New(),
		log:     log,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.handleChat)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /metrics", s.metrics.Handler())

	addr := ":" + env("PORT", "8080")
	log.Info("gateway listening", "addr", addr)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func (s *server) handleChat(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()
	reqID := r.Header.Get(api.HeaderRequestID)
	if reqID == "" {
		reqID = newID()
	}
	w.Header().Set(api.HeaderRequestID, reqID)

	key := strings.TrimPrefix(r.Header.Get(api.HeaderAPIKey), "Bearer ")
	if key == "" {
		writeErr(w, http.StatusUnauthorized, "bad_request", "missing API key")
		return
	}
	team := r.Header.Get(api.HeaderTeam)
	if team == "" {
		team = key
	}

	var req api.ChatRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || len(req.Messages) == 0 {
		writeErr(w, http.StatusBadRequest, "bad_request", "invalid request body")
		return
	}

	// 1. Rate limit. On a limiter (Redis) error the decision already reflects
	// the configured fail mode; we count the error and act on the decision.
	d, err := s.limiter.Allow(ctx, key)
	if err != nil {
		s.metrics.Inc("ratelimit_errors_total", 1)
		s.log.Warn("rate limiter error", "request_id", reqID, "err", err, "allowed", d.Allowed)
		if !d.Allowed { // fail closed
			s.metrics.Inc(`requests_total{result="limiter_unavailable"}`, 1)
			w.Header().Set("Retry-After", "1")
			writeErr(w, http.StatusServiceUnavailable, "limiter_unavailable", "rate limiter unavailable")
			return
		}
	} else if !d.Allowed {
		s.metrics.Inc(`requests_total{result="rate_limited"}`, 1)
		w.Header().Set("Retry-After", strconv.Itoa(int(d.RetryAfter.Seconds())+1))
		writeErr(w, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded")
		return
	}
	// 2. Route, then cache lookup (key includes the chosen model)
	model := s.router.Route(&req)
	ckey := cache.Key(model, req.Messages)
	cacheStatus := "BYPASS"
	if !req.NoCache {
		// A cache error (Redis slow/down) is counted and treated as a miss:
		// the cache never fails a request.
		resp, hit, err := s.cache.Get(ctx, ckey)
		if err != nil {
			s.metrics.Inc(`cache_errors_total{op="get"}`, 1)
			s.log.Warn("cache get error", "request_id", reqID, "err", err)
		}
		if hit {
			s.finish(w, reqID, key, team, model, "HIT", resp, 0, start)
			return
		}
		cacheStatus = "MISS"
	}
	// 3. Budget: only requests that will cost money are checked. On a backend
	// error ok already reflects BUDGET_FAIL_MODE.
	ok, err := s.budget.Check(ctx, team)
	if err != nil {
		s.metrics.Inc("budget_errors_total", 1)
		s.log.Warn("budget check error", "request_id", reqID, "team", team, "err", err, "allowed", ok)
		if !ok { // fail closed
			s.metrics.Inc(`requests_total{result="budget_unavailable"}`, 1)
			w.Header().Set("Retry-After", "1")
			writeErr(w, http.StatusServiceUnavailable, "budget_unavailable", "budget service unavailable")
			return
		}
	} else if !ok {
		s.metrics.Inc(`requests_total{result="budget_exceeded"}`, 1)
		writeErr(w, http.StatusPaymentRequired, "budget_exceeded", "team budget exhausted")
		return
	}
	// 4. Provider call. TODO(saravjit): on upstream 429, enqueue instead of failing.
	resp, err := s.provider.Complete(ctx, model, &req)
	if err != nil {
		status := http.StatusBadGateway
		var se *provider.StatusError
		if errors.As(err, &se) && se.Code == http.StatusTooManyRequests {
			status = http.StatusTooManyRequests
			s.metrics.Inc("upstream_429_total", 1)
		}
		s.metrics.Inc(`requests_total{result="upstream_error"}`, 1)
		s.log.Warn("provider error", "request_id", reqID, "model", model, "err", err)
		writeErr(w, status, "upstream_error", "provider request failed")
		return
	}
	cost := provider.Cost(model, resp.Usage)
	s.recordSpend(ctx, reqID, team, cost)
	if !req.NoCache {
		if err := s.cache.Set(ctx, ckey, resp, s.cacheTTL); err != nil {
			s.metrics.Inc(`cache_errors_total{op="set"}`, 1)
			s.log.Warn("cache set error", "request_id", reqID, "err", err)
		}
	}
	s.finish(w, reqID, key, team, model, cacheStatus, resp, cost, start)
}

func (s *server) finish(w http.ResponseWriter, reqID, key, team, model, cacheStatus string, resp *api.ChatResponse, cost float64, start time.Time) {
	lat := time.Since(start)
	w.Header().Set(api.HeaderCache, cacheStatus)
	w.Header().Set(api.HeaderModel, model)
	w.Header().Set(api.HeaderCostUSD, strconv.FormatFloat(cost, 'f', 6, 64))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)

	s.metrics.Inc(`requests_total{result="ok",cache="`+strings.ToLower(cacheStatus)+`",model="`+model+`"}`, 1)
	s.metrics.Inc(`cost_usd_total{model="`+model+`"}`, cost)
	s.metrics.Inc(`tokens_total{model="`+model+`"}`, float64(resp.Usage.TotalTokens))
	// One structured log line per request (schema: docs/api.md).
	s.log.Info("request", "request_id", reqID, "api_key", mask(key), "team", team, "model", model,
		"cache", cacheStatus, "tokens", resp.Usage.TotalTokens, "cost_usd", cost, "latency_ms", lat.Milliseconds())
}

// recordSpend charges the team. A failed write is logged with the amount so
// spend can be reconciled later; it never fails the request (already served).
func (s *server) recordSpend(ctx context.Context, reqID, team string, cost float64) {
	if cost <= 0 {
		return
	}
	s.metrics.Inc(`team_spend_usd_total{team="`+team+`"}`, cost)
	spend, err := s.budget.Add(ctx, team, cost)
	if err != nil {
		s.metrics.Inc("budget_errors_total", 1)
		s.log.Error("budget spend not recorded", "request_id", reqID, "team", team, "cost_usd", cost, "err", err)
		return
	}
	if spend.CrossedAlert {
		s.metrics.Inc(`budget_alerts_total{team="`+team+`"}`, 1)
		s.log.Warn("team budget alert", "team", team, "spent_usd", spend.TotalUSD, "limit_usd", spend.LimitUSD)
	}
}

// newBudget picks the budget tracker from config:
//
//	BUDGET_BACKEND           memory | redis (default: redis if REDIS_ADDR is set, else memory)
//	TEAM_BUDGET_USD          default monthly cap per team (0 = unlimited)
//	TEAM_BUDGETS             per-team caps, e.g. "team-a=50,team-b=10"
//	BUDGET_ALERT_FRACTION    alert when a team passes this share of its cap (default 0.8, 0 = off)
//	BUDGET_FAIL_MODE         open | closed — behaviour when Redis is unreachable
//	BUDGET_REDIS_TIMEOUT_MS  per-call budget (default 50)
func newBudget(log *slog.Logger, client *redis.Client) (budget.Tracker, error) {
	limits, err := budget.ParseLimits(os.Getenv("TEAM_BUDGETS"),
		envFloat("TEAM_BUDGET_USD", 0), envFloat("BUDGET_ALERT_FRACTION", 0.8))
	if err != nil {
		return nil, err
	}
	backend := os.Getenv("BUDGET_BACKEND")
	if backend == "" {
		backend = "memory"
		if client != nil {
			backend = "redis"
		}
	}
	switch backend {
	case "memory":
		log.Info("budget", "backend", "memory", "default_usd", limits.Default, "teams", len(limits.PerTeam))
		return budget.NewMemory(limits), nil
	case "redis":
		if client == nil {
			return nil, errors.New("BUDGET_BACKEND=redis requires REDIS_ADDR")
		}
		mode, err := budget.ParseFailMode(os.Getenv("BUDGET_FAIL_MODE"))
		if err != nil {
			return nil, err
		}
		timeout := time.Duration(envInt("BUDGET_REDIS_TIMEOUT_MS", 50)) * time.Millisecond
		log.Info("budget", "backend", "redis", "default_usd", limits.Default, "teams", len(limits.PerTeam),
			"fail_mode", os.Getenv("BUDGET_FAIL_MODE"), "timeout", timeout.String())
		return budget.NewRedis(client, limits, budget.WithFailMode(mode), budget.WithTimeout(timeout)), nil
	}
	return nil, fmt.Errorf("unknown BUDGET_BACKEND %q (want memory|redis)", backend)
}

// newLimiter picks the rate limiter from config:
//
//	RATELIMIT_BACKEND   local | redis (default: redis if REDIS_ADDR is set, else local)
//	RATELIMIT_FAIL_MODE open | closed  — behaviour when Redis is unreachable
//	RATELIMIT_REDIS_TIMEOUT_MS         — per-call budget before failing open/closed
func newLimiter(log *slog.Logger, client *redis.Client) (ratelimit.Limiter, error) {
	perMin, burst := envInt("RATE_LIMIT_PER_MIN", 100), envInt("RATE_LIMIT_BURST", 20)
	backend := os.Getenv("RATELIMIT_BACKEND")
	if backend == "" {
		backend = "local"
		if client != nil {
			backend = "redis"
		}
	}
	switch backend {
	case "local":
		log.Info("rate limiter", "backend", "local", "per_min", perMin, "burst", burst)
		return ratelimit.NewLocal(perMin, burst), nil
	case "redis":
		if client == nil {
			return nil, errors.New("RATELIMIT_BACKEND=redis requires REDIS_ADDR")
		}
		mode, err := ratelimit.ParseFailMode(os.Getenv("RATELIMIT_FAIL_MODE"))
		if err != nil {
			return nil, err
		}
		timeout := time.Duration(envInt("RATELIMIT_REDIS_TIMEOUT_MS", 50)) * time.Millisecond
		log.Info("rate limiter", "backend", "redis", "addr", client.Options().Addr, "per_min", perMin, "burst", burst,
			"fail_mode", os.Getenv("RATELIMIT_FAIL_MODE"), "timeout", timeout.String())
		return ratelimit.NewRedis(client, perMin, burst,
			ratelimit.WithFailMode(mode), ratelimit.WithTimeout(timeout)), nil
	}
	return nil, fmt.Errorf("unknown RATELIMIT_BACKEND %q (want local|redis)", backend)
}

// newCache picks the response cache from config:
//
//	CACHE_BACKEND          memory | redis (default: redis if REDIS_ADDR is set, else memory)
//	CACHE_REDIS_TIMEOUT_MS per-call budget before a lookup counts as a miss (default 50)
func newCache(log *slog.Logger, client *redis.Client) (cache.Cache, error) {
	backend := os.Getenv("CACHE_BACKEND")
	if backend == "" {
		backend = "memory"
		if client != nil {
			backend = "redis"
		}
	}
	switch backend {
	case "memory":
		log.Info("cache", "backend", "memory")
		return cache.NewMemory(), nil
	case "redis":
		if client == nil {
			return nil, errors.New("CACHE_BACKEND=redis requires REDIS_ADDR")
		}
		timeout := time.Duration(envInt("CACHE_REDIS_TIMEOUT_MS", 50)) * time.Millisecond
		log.Info("cache", "backend", "redis", "addr", client.Options().Addr, "timeout", timeout.String())
		return cache.NewRedis(client, cache.WithTimeout(timeout)), nil
	}
	return nil, fmt.Errorf("unknown CACHE_BACKEND %q (want memory|redis)", backend)
}

// newRedis builds the shared Redis client from REDIS_* env vars (see
// internal/redisx). It returns nil when REDIS_ADDR is unset. An unreachable
// Redis is only a warning: each component has its own failure mode.
func newRedis(log *slog.Logger) (*redis.Client, error) {
	client, err := redisx.FromEnv()
	if err != nil || client == nil {
		return nil, err
	}
	o := client.Options()
	if err := redisx.Ping(context.Background(), client, time.Second); err != nil {
		log.Warn("redis unreachable at startup", "addr", o.Addr, "err", err)
	} else {
		log.Info("redis connected", "addr", o.Addr, "pool_size", o.PoolSize)
	}
	return client, nil
}

func writeErr(w http.ResponseWriter, code int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(api.ErrorResponse{Error: api.ErrorBody{Type: typ, Message: msg}})
}

func mask(k string) string {
	if len(k) <= 4 {
		return "****"
	}
	return "****" + k[len(k)-4:]
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
		return v
	}
	return def
}
