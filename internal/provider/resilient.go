package provider

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
	"github.com/singhsaravjit/llm-gateway/internal/metrics"
)

var ErrCircuitOpen = errors.New("circuit breaker open")

type ResilientConfig struct {
	MaxAttempts     int           // attempts per model, including the first
	BaseBackoff     time.Duration // backoff before retry n is rand[0, min(Max, Base*2^n)]
	MaxBackoff      time.Duration
	BreakerFailures int // consecutive failures that open a model's breaker
	BreakerOpenFor  time.Duration
	Fallback        bool // cheap <-> strong when one of them fails
}

func DefaultResilientConfig() ResilientConfig {
	return ResilientConfig{MaxAttempts: 3, BaseBackoff: 100 * time.Millisecond, MaxBackoff: 2 * time.Second,
		BreakerFailures: 5, BreakerOpenFor: 10 * time.Second, Fallback: true}
}

// Resilient wraps a Client with retries, a circuit breaker per model and
// fallback between the cheap and strong models.
//
// For cheap/strong requests the returned resp.Model is always the model that
// actually answered ("cheap" or "strong"), so the caller can see a fallback
// even after the response went through the Redis queue as JSON.
type Resilient struct {
	next Client
	cfg  ResilientConfig
	reg  *metrics.Registry

	mu       sync.Mutex
	breakers map[string]*Breaker
}

func NewResilient(next Client, cfg ResilientConfig, reg *metrics.Registry) *Resilient {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if reg == nil {
		reg = metrics.New()
	}
	r := &Resilient{next: next, cfg: cfg, reg: reg, breakers: map[string]*Breaker{}}
	for m := range Pricing { // so the breaker panel shows both models from the start
		r.breaker(m)
	}
	return r
}

// FallbackModel returns the model to fall back to, if there is one.
func FallbackModel(model string) (string, bool) {
	switch model {
	case "cheap":
		return "strong", true
	case "strong":
		return "cheap", true
	}
	return "", false
}

func (r *Resilient) breaker(model string) *Breaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.breakers[model]
	if !ok {
		gauge := `breaker_state{model="` + model + `"}`
		b = &Breaker{Threshold: r.cfg.BreakerFailures, OpenFor: r.cfg.BreakerOpenFor, OnChange: func(s BreakerState) {
			r.reg.Set(gauge, float64(s))
			if s == Open {
				r.reg.Inc(`breaker_opens_total{model="`+model+`"}`, 1)
			}
		}}
		r.reg.Set(gauge, 0)
		r.breakers[model] = b
	}
	return b
}

func (r *Resilient) Complete(ctx context.Context, model string, req *api.ChatRequest) (*api.ChatResponse, error) {
	other, pair := FallbackModel(model)
	resp, err := r.try(ctx, model, req)
	if err == nil {
		if pair {
			resp.Model = model
		}
		return resp, nil
	}
	if !pair || !r.cfg.Fallback || ctx.Err() != nil || !(isFailure(ctx, err) || errors.Is(err, ErrCircuitOpen)) {
		return nil, err // 4xx / 429 / cancelled go back unchanged
	}
	resp, ferr := r.try(ctx, other, req)
	if ferr != nil {
		// wrap ferr so a 429 from the fallback model still reaches the queue as a 429
		return nil, fmt.Errorf("%s: %v; fallback to %s: %w", model, err, other, ferr)
	}
	r.reg.Inc(`provider_fallbacks_total{from="`+model+`",to="`+other+`"}`, 1)
	resp.Model = other
	return resp, nil
}

// try calls one model with retries, respecting its breaker.
func (r *Resilient) try(ctx context.Context, model string, req *api.ChatRequest) (*api.ChatResponse, error) {
	b := r.breaker(model)
	var lastErr error
	for attempt := 0; attempt < r.cfg.MaxAttempts; attempt++ {
		if attempt > 0 && !sleep(ctx, r.backoff(attempt-1)) {
			return nil, lastErr
		}
		if !b.Allow() {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, fmt.Errorf("%s: %w", model, ErrCircuitOpen)
		}
		if attempt > 0 {
			r.reg.Inc(`provider_retries_total{model="`+model+`"}`, 1)
		}
		resp, err := r.next.Complete(ctx, model, req)
		if err == nil {
			b.Success()
			return resp, nil
		}
		if !isFailure(ctx, err) {
			b.Release()
			return nil, err
		}
		b.Failure()
		lastErr = err
	}
	return nil, lastErr
}

// backoff is exponential with full jitter.
func (r *Resilient) backoff(retry int) time.Duration {
	d := r.cfg.MaxBackoff
	if retry < 20 { // avoid overflowing the shift
		d = min(d, r.cfg.BaseBackoff<<retry)
	}
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d) + 1))
}

// sleep waits d. It returns false straight away if ctx would expire before
// the wait is over, since the retry could not finish anyway.
func sleep(ctx context.Context, d time.Duration) bool {
	if dl, ok := ctx.Deadline(); ok && time.Now().Add(d).After(dl) {
		return false
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// isFailure: 5xx, timeouts and connection errors count against the provider.
// 4xx (including 429) and the caller's own context ending do not.
func isFailure(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se.Code >= 500
	}
	var ne net.Error // url.Error from http.Client covers refused connections and Client.Timeout
	return errors.As(err, &ne) || errors.Is(err, context.DeadlineExceeded)
}
