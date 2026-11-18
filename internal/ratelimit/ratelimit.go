// Package ratelimit — owner: Saravjit.
//
// Distributed token-bucket rate limiter per API key. The limit must hold
// across ALL gateway nodes, so the production implementation keeps bucket
// state in Redis (atomic Lua script). The local implementation is per-node
// and is the "local counters" baseline for Experiment 3.
// TODO(saravjit): Redis token bucket (Lua), fail-open/fail-closed policy for
// Experiment 4, metrics for over-admission.
package ratelimit

import (
	"context"
	"sync"
	"time"
)

type Decision struct {
	Allowed    bool
	Remaining  float64
	RetryAfter time.Duration
}

type Limiter interface {
	Allow(ctx context.Context, key string) (Decision, error)
}

// Local is an in-process token bucket. Each node has its own buckets, so with
// N nodes a client can get up to N x the limit — that is the point of Exp. 3.
type Local struct {
	rate  float64 // tokens per second
	burst float64
	mu    sync.Mutex
	b     map[string]*bucket
	now   func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func NewLocal(perMinute, burst int) *Local {
	return &Local{rate: float64(perMinute) / 60, burst: float64(burst), b: map[string]*bucket{}, now: time.Now}
}

func (l *Local) Allow(_ context.Context, key string) (Decision, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	bk, ok := l.b[key]
	if !ok {
		bk = &bucket{tokens: l.burst, last: now}
		l.b[key] = bk
	}
	bk.tokens = min(l.burst, bk.tokens+now.Sub(bk.last).Seconds()*l.rate)
	bk.last = now
	if bk.tokens >= 1 {
		bk.tokens--
		return Decision{Allowed: true, Remaining: bk.tokens}, nil
	}
	wait := time.Duration((1 - bk.tokens) / l.rate * float64(time.Second))
	return Decision{Allowed: false, RetryAfter: wait}, nil
}
