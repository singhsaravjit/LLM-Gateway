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
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
	"github.com/singhsaravjit/llm-gateway/internal/budget"
	"github.com/singhsaravjit/llm-gateway/internal/cache"
	"github.com/singhsaravjit/llm-gateway/internal/metrics"
	"github.com/singhsaravjit/llm-gateway/internal/provider"
	"github.com/singhsaravjit/llm-gateway/internal/queue"
	"github.com/singhsaravjit/llm-gateway/internal/ratelimit"
	"github.com/singhsaravjit/llm-gateway/internal/redisx"
	"github.com/singhsaravjit/llm-gateway/internal/router"

	"github.com/redis/go-redis/v9"
)

type server struct {
	limiter    ratelimit.Limiter
	budget     budget.Tracker
	cache      cache.Cache
	cacheOn    bool // CACHE_ENABLED; false skips the cache for every request
	cacheTTL   time.Duration
	reqTimeout time.Duration    // REQUEST_TIMEOUT_MS for direct provider calls; 0 = no limit
	coalesce   *cache.Coalescer // CACHE_COALESCE; groups identical in-flight misses
	router     router.Router
	provider   provider.Client
	queue      queue.Queue // nil when QUEUE_ENABLED=false
	pacer      queue.Pacer
	metrics    *metrics.Registry
	log        *slog.Logger
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("invalid config", "err", err)
		os.Exit(1)
	}
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
	reg := metrics.New()
	log.Info("cache settings", "enabled", cfg.CacheEnabled, "ttl", cfg.CacheTTL.String())
	for m := range provider.Pricing { // so the dollars-saved panel starts at 0
		reg.Inc(`cost_saved_usd_total{model="`+m+`"}`, 0)
	}
	// One Resilient client shared by the direct path and the queue workers, so
	// both get retries, breakers and fallback.
	prov := provider.NewResilient(provider.NewHTTP(cfg.ProviderURL, cfg.ProviderAPIKey, cfg.ProviderTimeout),
		provider.ResilientConfig{MaxAttempts: cfg.RetryAttempts, BaseBackoff: cfg.RetryBase, MaxBackoff: cfg.RetryMax,
			BreakerFailures: cfg.BreakerFailures, BreakerOpenFor: cfg.BreakerOpen, Fallback: cfg.FallbackEnabled}, reg)
	log.Info("provider resilience", "attempts", cfg.RetryAttempts, "base_backoff", cfg.RetryBase.String(),
		"max_backoff", cfg.RetryMax.String(), "breaker_failures", cfg.BreakerFailures,
		"breaker_open", cfg.BreakerOpen.String(), "fallback", cfg.FallbackEnabled)
	q, pacer, err := newQueue(log, rdb, prov, reg)
	if err != nil {
		log.Error("queue config", "err", err)
		os.Exit(1)
	}
	coalescer, err := newCoalescer(log, rdb, respCache, reg, cfg)
	if err != nil {
		log.Error("coalescing config", "err", err)
		os.Exit(1)
	}
	s := &server{
		limiter:    limiter,
		budget:     budgets,
		cache:      cache.NewInstrumented(respCache, reg),
		cacheOn:    cfg.CacheEnabled,
		cacheTTL:   cfg.CacheTTL,
		reqTimeout: cfg.RequestTimeout,
		coalesce:   coalescer,
		router:     router.LengthRouter{MaxCheapChars: cfg.MaxCheapChars},
		provider:   prov,
		queue:      q,
		pacer:      pacer,
		metrics:    reg,
		log:        log,
	}
	if q != nil {
		go s.reportQueueDepth()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.handleChat)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /metrics", s.metrics.Handler())

	addr := ":" + cfg.Port
	log.Info("gateway listening", "addr", addr)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	// ECS sends SIGTERM when a task is replaced; Ctrl+C sends an interrupt locally.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	select {
	case err := <-errCh:
		log.Error("server stopped", "err", err)
		os.Exit(1)
	case <-ctx.Done():
	}

	// Stop accepting new connections and wait for in-flight requests to finish.
	log.Info("shutting down", "drain_timeout", cfg.ShutdownTimeout.String())
	sctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	err = srv.Shutdown(sctx)
	cancel()
	if err != nil {
		log.Error("shutdown timed out, some requests were cut off", "err", err)
		os.Exit(1)
	}
	log.Info("gateway stopped")
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
	limStart := time.Now()
	d, err := s.limiter.Allow(ctx, key)
	s.metrics.Inc("ratelimit_check_seconds_sum", time.Since(limStart).Seconds()) // Experiment 3: cost of the check
	s.metrics.Inc("ratelimit_check_seconds_count", 1)
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
	ckey := cache.Key(model, req.Messages, req.MaxTokens)
	cacheStatus := "BYPASS"
	useCache := s.cacheOn && !req.NoCache
	if useCache {
		// A cache error (Redis slow/down) is treated as a miss: the cache
		// never fails a request. s.cache (Instrumented) counts hits, misses,
		// errors and lookup latency.
		resp, hit, err := s.cache.Get(ctx, ckey)
		if err != nil {
			s.log.Warn("cache get error", "request_id", reqID, "err", err)
		}
		if hit {
			// The hit cost nothing; what it saved is what the provider
			// would have charged for the same usage.
			s.finish(w, reqID, key, team, model, "HIT", resp, 0, provider.Cost(model, resp.Usage), start)
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
	// 4. Provider call, through the overflow queue when the provider is at
	// its limit. On a miss it also goes through the coalescer, so identical
	// requests in flight share one provider call (CACHE_COALESCE). fetch
	// stores the answer before returning, so waiters on other nodes find it
	// in the cache.
	fetch := func(fctx context.Context) (*api.ChatResponse, error) {
		resp, err := s.callProvider(fctx, reqID, model, &req)
		// A fallback answer is not what the routed model would have said, so it
		// is not cached under that model's key (other nodes' waiters then fetch
		// for themselves once their wait runs out).
		if err == nil && useCache && modelUsed(model, resp) == model {
			if err := s.cache.Set(fctx, ckey, resp, s.cacheTTL); err != nil {
				s.log.Warn("cache set error", "request_id", reqID, "err", err)
			}
		}
		return resp, err
	}
	var resp *api.ChatResponse
	outcome := cache.Fetched
	if useCache {
		resp, outcome, err = s.coalesce.Do(ctx, ckey, fetch)
	} else {
		resp, err = fetch(ctx)
	}
	switch {
	case errors.Is(err, queue.ErrFull):
		s.metrics.Inc(`requests_total{result="queue_full"}`, 1)
		w.Header().Set("Retry-After", "1")
		writeErr(w, http.StatusServiceUnavailable, "queue_full", "overflow queue is full")
		return
	case errors.Is(err, queue.ErrTimeout):
		s.metrics.Inc(`requests_total{result="queue_timeout"}`, 1)
		writeErr(w, http.StatusGatewayTimeout, "queue_timeout", "timed out waiting for provider capacity")
		return
	case ctx.Err() != nil:
		s.metrics.Inc(`requests_total{result="client_gone"}`, 1)
		return // client disconnected; nothing to write
	}
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
	// Local waiters share the leader's resp, so they see the fallback too.
	used := modelUsed(model, resp)
	if used != model {
		s.log.Info("served by fallback model", "request_id", reqID, "routed", model, "used", used)
	}
	if outcome != cache.Fetched {
		// Another request paid for this answer; this one waited for it.
		s.finish(w, reqID, key, team, used, "COALESCED", resp, 0, provider.Cost(used, resp.Usage), start)
		return
	}
	cost := provider.Cost(used, resp.Usage)
	s.recordSpend(ctx, reqID, team, cost)
	s.finish(w, reqID, key, team, used, cacheStatus, resp, cost, 0, start)
}

// modelUsed is the model that actually answered. After a fallback
// provider.Resilient sets resp.Model to the other model of the cheap/strong
// pair; this also works for queued requests since resp.Model is in the JSON.
func modelUsed(routed string, resp *api.ChatResponse) string {
	if other, ok := provider.FallbackModel(routed); ok && resp.Model == other {
		return other
	}
	return routed
}

func (s *server) finish(w http.ResponseWriter, reqID, key, team, model, cacheStatus string, resp *api.ChatResponse, cost, saved float64, start time.Time) {
	lat := time.Since(start)
	w.Header().Set(api.HeaderCache, cacheStatus)
	w.Header().Set(api.HeaderModel, model)
	w.Header().Set(api.HeaderCostUSD, strconv.FormatFloat(cost, 'f', 6, 64))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)

	s.metrics.Inc(`requests_total{result="ok",cache="`+strings.ToLower(cacheStatus)+`",model="`+model+`"}`, 1)
	s.metrics.Inc(`cost_usd_total{model="`+model+`"}`, cost)
	s.metrics.Inc(`tokens_total{model="`+model+`"}`, float64(resp.Usage.TotalTokens))
	if saved > 0 {
		s.metrics.Inc(`cost_saved_usd_total{model="`+model+`"}`, saved)
	}
	// One structured log line per request (schema: docs/api.md).
	s.log.Info("request", "request_id", reqID, "api_key", mask(key), "team", team, "model", model,
		"cache", cacheStatus, "tokens", resp.Usage.TotalTokens, "cost_usd", cost, "cost_saved_usd", saved,
		"latency_ms", lat.Milliseconds())
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

// callProvider calls the provider directly while the shared provider rate
// (pacer) has room. When it doesn't, or the provider answers 429, the request
// waits in the overflow queue. With the queue disabled (Experiment 5 baseline)
// every request goes straight to the provider and 429s reach the client.
func (s *server) callProvider(ctx context.Context, reqID, model string, req *api.ChatRequest) (*api.ChatResponse, error) {
	if s.queue == nil {
		return s.complete(ctx, model, req)
	}
	if s.pacer.TryAcquire(ctx, model) {
		resp, err := s.complete(ctx, model, req)
		if !queue.IsRateLimited(err) {
			return resp, err
		}
		s.metrics.Inc("upstream_429_total", 1) // pacer allowed it but the provider disagreed: queue it
	}
	resp, err := s.queue.Submit(ctx, model, req)
	if errors.Is(err, queue.ErrUnavailable) { // Redis down: fail open, call the provider directly
		s.metrics.Inc("queue_errors_total", 1)
		s.log.Warn("queue unavailable, calling provider directly", "request_id", reqID, "err", err)
		return s.complete(ctx, model, req)
	}
	return resp, err
}

// complete calls the provider directly with a per-request timeout. Queued
// requests are bounded by QUEUE_WAIT_TIMEOUT_MS instead.
func (s *server) complete(ctx context.Context, model string, req *api.ChatRequest) (*api.ChatResponse, error) {
	if s.reqTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.reqTimeout)
		defer cancel()
	}
	return s.provider.Complete(ctx, model, req)
}

func (s *server) reportQueueDepth() {
	for range time.Tick(2 * time.Second) {
		if n, err := s.queue.Depth(context.Background()); err == nil {
			s.metrics.Set("queue_depth", float64(n))
		}
	}
}

// newQueue builds the overflow queue and the shared provider pacer:
//
//	QUEUE_ENABLED               true | false (default true; false = Experiment 5 baseline)
//	QUEUE_BACKEND               memory | redis (default: redis if REDIS_ADDR is set, else memory)
//	QUEUE_MAX                   queue bound, all nodes together for redis (default 1000)
//	QUEUE_WAIT_TIMEOUT_MS       how long a client may wait (default 30000)
//	QUEUE_WORKERS               workers per node (default 8)
//	QUEUE_VISIBILITY_TIMEOUT_MS reclaim jobs held this long by a crashed worker (default wait + 30s)
//	PROVIDER_RPS                provider rate shared by all nodes (default 45, just under the mock's 50; 0 = no pacing)
//	PROVIDER_BURST              default = PROVIDER_RPS
//	PROVIDER_RPS_PER_MODEL      true = one bucket per model (real providers); default false (the mock has one limit)
//	NODE_ID                     unique per gateway process (default hostname + random suffix)
func newQueue(log *slog.Logger, client *redis.Client, prov provider.Client, reg *metrics.Registry) (queue.Queue, queue.Pacer, error) {
	if v := os.Getenv("QUEUE_ENABLED"); v == "false" || v == "0" {
		log.Info("queue", "enabled", false)
		return nil, nil, nil
	}
	backend := os.Getenv("QUEUE_BACKEND")
	if backend == "" {
		backend = "memory"
		if client != nil {
			backend = "redis"
		}
	}
	rps := envInt("PROVIDER_RPS", 45)
	var pacer queue.Pacer = queue.Unpaced{}
	if rps > 0 {
		burst := envInt("PROVIDER_BURST", rps)
		var lim ratelimit.Limiter = ratelimit.NewLocal(rps*60, burst)
		if backend == "redis" && client != nil {
			lim = ratelimit.NewRedis(client, rps*60, burst, ratelimit.WithKeyPrefix("rl:provider:"),
				ratelimit.WithFailMode(ratelimit.FailOpen))
		}
		pacer = queue.LimiterPacer{Limiter: lim, PerModel: os.Getenv("PROVIDER_RPS_PER_MODEL") == "true"}
	}
	cfg := queue.Config{
		Max:         int64(envInt("QUEUE_MAX", 1000)),
		Workers:     envInt("QUEUE_WORKERS", 8),
		WaitTimeout: time.Duration(envInt("QUEUE_WAIT_TIMEOUT_MS", 30000)) * time.Millisecond,
		Exec:        prov.Complete,
		Pacer:       pacer,
		Obs:         reg,
	}
	switch backend {
	case "memory":
		log.Info("queue", "backend", "memory", "max", cfg.Max, "workers", cfg.Workers, "provider_rps", rps)
		return queue.NewMemory(cfg), pacer, nil
	case "redis":
		if client == nil {
			return nil, nil, errors.New("QUEUE_BACKEND=redis requires REDIS_ADDR")
		}
		node := env("NODE_ID", hostname()+"-"+newID()[:6])
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		q, err := queue.NewRedis(ctx, client, queue.RedisConfig{Config: cfg, NodeID: node,
			Visibility: time.Duration(envInt("QUEUE_VISIBILITY_TIMEOUT_MS", 0)) * time.Millisecond})
		if err != nil {
			return nil, nil, err
		}
		log.Info("queue", "backend", "redis", "node", node, "max", cfg.Max, "workers", cfg.Workers, "provider_rps", rps)
		return q, pacer, nil
	}
	return nil, nil, fmt.Errorf("unknown QUEUE_BACKEND %q (want memory|redis)", backend)
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "gateway"
	}
	return h
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

// newCoalescer picks request coalescing from config:
//
//	CACHE_COALESCE          off | local | redis (default off)
//	                        local: one provider call per key per node (singleflight)
//	                        redis: one per key per cluster (lock:<key> in Redis); needs CACHE_BACKEND=redis
//	CACHE_COALESCE_WAIT_MS  how long a waiter polls for another node's answer before calling the provider (default 5000)
//	CACHE_COALESCE_POLL_MS  poll interval (default 50)
//
// The lock TTL is REQUEST_TIMEOUT_MS + 1 s (the longest a direct provider call
// can take), so a crashed holder frees the key.
func newCoalescer(log *slog.Logger, client *redis.Client, c cache.Cache, reg *metrics.Registry, cfg config) (*cache.Coalescer, error) {
	mode, err := cache.ParseCoalesceMode(os.Getenv("CACHE_COALESCE"))
	if err != nil {
		return nil, err
	}
	if mode != cache.CoalesceRedis {
		log.Info("coalescing", "mode", mode)
		return cache.NewCoalescer(mode, c, reg)
	}
	if _, ok := c.(*cache.Redis); !ok || client == nil {
		return nil, errors.New("CACHE_COALESCE=redis requires REDIS_ADDR and the redis cache backend")
	}
	if !cfg.CacheEnabled || cfg.CacheTTL <= 0 {
		return nil, errors.New("CACHE_COALESCE=redis needs the cache on with CACHE_TTL_SECONDS > 0 (waiters read the answer from the cache)")
	}
	lockTTL := cfg.RequestTimeout + time.Second // covers a direct provider call with its retries
	wait := time.Duration(envInt("CACHE_COALESCE_WAIT_MS", 5000)) * time.Millisecond
	poll := time.Duration(envInt("CACHE_COALESCE_POLL_MS", 50)) * time.Millisecond
	timeout := time.Duration(envInt("CACHE_REDIS_TIMEOUT_MS", 50)) * time.Millisecond
	log.Info("coalescing", "mode", mode, "lock_ttl", lockTTL.String(), "wait", wait.String(), "poll", poll.String())
	return cache.NewCoalescer(mode, c, reg, cache.WithRedisLock(client, lockTTL),
		cache.WithWait(wait, poll), cache.WithLockTimeout(timeout))
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
