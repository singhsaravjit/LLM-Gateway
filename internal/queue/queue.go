// Package queue — owner: Saravjit.
//
// Overflow queue for requests that would exceed the LLM provider's rate limit.
// Design: docs/design/queue-and-budgets.md (section 1).
//
// A Pacer shares the provider's rate across all gateway nodes. The gateway
// calls the provider directly while the pacer has capacity (fast path, no
// queueing); otherwise — or when the provider answers 429 — it Submits the
// request and waits. Workers drain the queue only as fast as the pacer allows.
// When the queue is full, Submit returns ErrFull at once (backpressure).
//
// Two implementations share the same worker logic:
//   - Redis: a Redis Stream drained by workers on every node, with results sent
//     back to the waiting node over its own reply stream (production).
//   - Memory: a channel and workers inside one process (dev without Redis, and
//     the documented fallback).
package queue

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
	"github.com/singhsaravjit/llm-gateway/internal/provider"
	"github.com/singhsaravjit/llm-gateway/internal/ratelimit"
)

var (
	// ErrFull: the queue is at its limit; reject the request now (503 queue_full).
	ErrFull = errors.New("queue full")
	// ErrTimeout: the request waited longer than WaitTimeout (504 queue_timeout).
	ErrTimeout = errors.New("queue wait timeout")
	// ErrUnavailable: the queue backend (Redis) failed; the request was not
	// queued. The gateway fails open and calls the provider directly.
	ErrUnavailable = errors.New("queue unavailable")
)

// Executor performs one provider call (provider.Client.Complete).
type Executor func(ctx context.Context, model string, req *api.ChatRequest) (*api.ChatResponse, error)

// Observer receives counters; *metrics.Registry satisfies it.
type Observer interface {
	Inc(name string, v float64)
}

type nopObserver struct{}

func (nopObserver) Inc(string, float64) {}

// Queue accepts requests that must wait for provider capacity.
type Queue interface {
	// Submit enqueues the request and blocks until a worker returns the
	// provider's answer, ctx is done, or WaitTimeout passes.
	Submit(ctx context.Context, model string, req *api.ChatRequest) (*api.ChatResponse, error)
	// Depth is the number of requests waiting or being worked on (all nodes for Redis).
	Depth(ctx context.Context) (int64, error)
	// Close stops this node's workers.
	Close() error
}

// Config is shared by both implementations.
type Config struct {
	Max         int64         // queue bound across all nodes (Redis) or this node (Memory)
	Workers     int           // workers on this node; 0 = this node only submits (Redis)
	WaitTimeout time.Duration // how long a client may wait in the queue
	Exec        Executor
	Pacer       Pacer
	Obs         Observer
}

func (c *Config) defaults() {
	if c.Max <= 0 {
		c.Max = 1000
	}
	if c.WaitTimeout <= 0 {
		c.WaitTimeout = 30 * time.Second
	}
	if c.Pacer == nil {
		c.Pacer = Unpaced{}
	}
	if c.Obs == nil {
		c.Obs = nopObserver{}
	}
}

// Pacer is the provider-side rate limit shared by the fast path and workers.
type Pacer interface {
	// TryAcquire takes capacity now if there is any, without waiting.
	TryAcquire(ctx context.Context, model string) bool
	// Wait blocks until capacity is available or ctx is done.
	Wait(ctx context.Context, model string) error
}

// Unpaced never limits (used when no provider rate is configured).
type Unpaced struct{}

func (Unpaced) TryAcquire(context.Context, string) bool  { return true }
func (Unpaced) Wait(ctx context.Context, _ string) error { return ctx.Err() }

// LimiterPacer paces with a token bucket — ratelimit.Redis (shared by all
// nodes, key rl:provider:<scope>) or ratelimit.Local (one node).
type LimiterPacer struct {
	Limiter  ratelimit.Limiter
	PerModel bool          // one bucket per model; false = one bucket for the provider
	MaxSleep time.Duration // cap on one wait step (default 250ms)
}

func (p LimiterPacer) key(model string) string {
	if p.PerModel {
		return model
	}
	return "all"
}

func (p LimiterPacer) TryAcquire(ctx context.Context, model string) bool {
	d, _ := p.Limiter.Allow(ctx, p.key(model)) // on limiter error the decision follows its fail mode
	return d.Allowed
}

func (p LimiterPacer) Wait(ctx context.Context, model string) error {
	maxSleep := p.MaxSleep
	if maxSleep <= 0 {
		maxSleep = 250 * time.Millisecond
	}
	for {
		d, _ := p.Limiter.Allow(ctx, p.key(model))
		if d.Allowed {
			return nil
		}
		wait := min(max(d.RetryAfter, time.Millisecond), maxSleep)
		wait += time.Duration(rand.Int64N(int64(wait)/4 + 1)) // jitter: workers on many nodes don't retry in lockstep
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// IsRateLimited reports whether err is a provider 429.
func IsRateLimited(err error) bool {
	var se *provider.StatusError
	return errors.As(err, &se) && se.Code == http.StatusTooManyRequests
}

// work runs one job: wait for provider capacity, call it, and on a 429 back
// off and try again until the job's deadline. Shared by both implementations.
func work(ctx context.Context, cfg *Config, model string, req *api.ChatRequest, deadline time.Time) (*api.ChatResponse, error) {
	if !time.Now().Before(deadline) {
		cfg.Obs.Inc(`queue_rejected_total{reason="expired"}`, 1)
		return nil, ErrTimeout // the client has already given up: don't spend money
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := cfg.Pacer.Wait(ctx, model); err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			cfg.Obs.Inc(`queue_rejected_total{reason="expired"}`, 1)
			return nil, ErrTimeout
		}
		resp, err := cfg.Exec(ctx, model, req)
		if err != nil && ctx.Err() != nil {
			// The deadline passed during the provider call: the client is
			// timing out too, so report a queue timeout, not a provider error.
			cfg.Obs.Inc(`queue_rejected_total{reason="expired"}`, 1)
			return nil, ErrTimeout
		}
		if !IsRateLimited(err) {
			return resp, err
		}
		lastErr = err
		cfg.Obs.Inc("queue_upstream_429_total", 1)
		select {
		case <-time.After(backoff(attempt)):
		case <-ctx.Done():
			return nil, lastErr
		}
	}
}

// backoff: exponential with full jitter, 50ms base, capped at 2s.
func backoff(attempt int) time.Duration {
	d := 50 * time.Millisecond << min(attempt, 6)
	d = min(d, 2*time.Second)
	return time.Duration(rand.Int64N(int64(d)) + 1)
}

// deadlineFor is the earlier of ctx's deadline and now + WaitTimeout.
func deadlineFor(ctx context.Context, wait time.Duration) time.Time {
	d := time.Now().Add(wait)
	if cd, ok := ctx.Deadline(); ok && cd.Before(d) {
		return cd
	}
	return d
}
