package provider

import (
	"sync"
	"time"
)

type BreakerState int

const (
	Closed BreakerState = iota
	Open
	HalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	}
	return "closed"
}

// Breaker is a per-model circuit breaker. It opens after Threshold
// consecutive failures, rejects calls for OpenFor, then lets a single probe
// through (half-open). The probe's result closes or reopens it.
type Breaker struct {
	Threshold int
	OpenFor   time.Duration
	OnChange  func(BreakerState) // called on every state change (metrics)

	mu       sync.Mutex
	state    BreakerState
	failures int
	openedAt time.Time
	now      func() time.Time
}

func (b *Breaker) clock() time.Time {
	if b.now != nil {
		return b.now()
	}
	return time.Now()
}

func (b *Breaker) set(s BreakerState) {
	if b.state == s {
		return
	}
	b.state = s
	if b.OnChange != nil {
		b.OnChange(s)
	}
}

func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow reports whether a call may go to the provider now. When the open
// period is over, the first caller becomes the half-open probe and everyone
// else is still rejected until the probe reports back.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return true
	case Open:
		if b.clock().Sub(b.openedAt) < b.OpenFor {
			return false
		}
		b.set(HalfOpen)
		return true
	}
	return false // half-open: a probe is already in flight
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Open {
		return // late result from a call that started before the breaker opened
	}
	b.failures = 0
	b.set(Closed)
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		b.failures++
		if b.failures >= b.Threshold {
			b.open()
		}
	case HalfOpen:
		b.open()
	}
}

// Release is for a call that told us nothing about provider health (4xx,
// 429, caller cancelled). If it was the probe, the next caller probes again.
func (b *Breaker) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == HalfOpen {
		b.set(Open) // openedAt is already old enough, so the next Allow probes
	}
}

func (b *Breaker) open() {
	b.failures = 0
	b.openedAt = b.clock()
	b.set(Open)
}
