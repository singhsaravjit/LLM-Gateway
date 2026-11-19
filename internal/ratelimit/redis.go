package ratelimit

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed token_bucket.lua
var tokenBucketLua string

var tokenBucket = redis.NewScript(tokenBucketLua)

// FailMode decides what happens when Redis is slow or unreachable (Experiment 4).
type FailMode int

const (
	// FailOpen lets requests through: the service stays available, limits are not enforced (AP).
	FailOpen FailMode = iota
	// FailClosed rejects requests: limits stay correct, the service is unavailable (CP).
	FailClosed
)

func ParseFailMode(s string) (FailMode, error) {
	switch s {
	case "", "open":
		return FailOpen, nil
	case "closed":
		return FailClosed, nil
	}
	return 0, fmt.Errorf("unknown fail mode %q (want open|closed)", s)
}

// Redis is a token bucket shared by every gateway node. Bucket state lives in
// the hash rl:<key> {tokens, ts} and is updated atomically by a Lua script
// that reads the clock from Redis, so node clock skew cannot affect limits.
type Redis struct {
	client   redis.Scripter
	rate     float64 // tokens per second
	burst    float64
	failMode FailMode
	timeout  time.Duration // per-call budget; keeps a slow Redis from stalling requests
	prefix   string
}

type RedisOption func(*Redis)

func WithFailMode(m FailMode) RedisOption     { return func(r *Redis) { r.failMode = m } }
func WithTimeout(d time.Duration) RedisOption { return func(r *Redis) { r.timeout = d } }
func WithKeyPrefix(p string) RedisOption      { return func(r *Redis) { r.prefix = p } }

// NewRedis builds a limiter allowing perMinute requests per key, with bursts up to burst.
// client is any go-redis client (single node, cluster or ring).
func NewRedis(client redis.Scripter, perMinute, burst int, opts ...RedisOption) *Redis {
	r := &Redis{
		client:   client,
		rate:     float64(perMinute) / 60,
		burst:    float64(burst),
		failMode: FailOpen,
		timeout:  50 * time.Millisecond,
		prefix:   "rl:",
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Allow takes one token for key. On a Redis error it returns the error together
// with the decision dictated by the fail mode, so callers can count failures
// and still act on Decision.
func (r *Redis) Allow(ctx context.Context, key string) (Decision, error) {
	return r.AllowN(ctx, key, 1)
}

// AllowN takes n tokens (e.g. to charge by expected tokens instead of requests).
func (r *Redis) AllowN(ctx context.Context, key string, n float64) (Decision, error) {
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}
	res, err := tokenBucket.Run(ctx, r.client, []string{r.prefix + key},
		r.rate, r.burst, n).StringSlice()
	if err != nil {
		return r.failDecision(), fmt.Errorf("ratelimit redis: %w", err)
	}
	if len(res) != 3 {
		return r.failDecision(), fmt.Errorf("ratelimit redis: unexpected reply %v", res)
	}
	remaining, _ := strconv.ParseFloat(res[1], 64)
	retry, _ := strconv.ParseFloat(res[2], 64)
	return Decision{
		Allowed:    res[0] == "1",
		Remaining:  remaining,
		RetryAfter: time.Duration(retry * float64(time.Second)),
	}, nil
}

func (r *Redis) failDecision() Decision {
	if r.failMode == FailOpen {
		return Decision{Allowed: true}
	}
	return Decision{Allowed: false, RetryAfter: time.Second}
}
