// Package redisx — owner: Dinesh.
//
// Builds the one go-redis client the gateway shares between the cache, rate
// limiter, budgets and queue, so every component uses the same connection pool
// and timeout settings.
//
// In the gateway (cmd/gateway builds it once and passes it around):
//
//	client, err := redisx.FromEnv() // nil, nil when REDIS_ADDR is unset
//	if client != nil {
//		limiter = ratelimit.NewRedis(client, perMin, burst)
//	}
//
// Components take a client and set a short per-call timeout with
// context.WithTimeout; the client honours it (see New).
//
// In tests, use the redistest subpackage: redistest.Start(t) for a miniredis
// plus client, redistest.NewFaultProxy to make Redis slow or down.
package redisx

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds the client settings. Zero values mean "use the go-redis default".
type Config struct {
	Addr         string
	PoolSize     int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// MaxRetries is how many times a failed command is retried. 0 (the
	// default) means no retries; see New.
	MaxRetries int
}

// New builds a client for cfg. It does not connect; go-redis dials lazily on
// the first command. Use Ping to check reachability.
//
// ContextTimeoutEnabled is always on: without it go-redis ignores context
// deadlines on socket reads/writes, so a per-call context.WithTimeout (the
// limiter's and cache's 50 ms budget) would not stop a slow Redis from
// blocking a request for the full ReadTimeout (3 s by default).
//
// Retries are off unless cfg.MaxRetries > 0. go-redis retries 3 times by
// default with a backoff of 8 to 512 ms, so with Redis down a command that
// fails at once (connection refused) kept retrying until the caller's 50 ms
// budget ran out: every Redis call cost the full 50 ms. Every caller already
// has its own fallback (fail open or closed), so failing fast is better.
func New(cfg Config) *redis.Client {
	retries := cfg.MaxRetries
	if retries <= 0 {
		retries = -1 // go-redis: 0 means "default (3)", -1 means none
	}
	return redis.NewClient(&redis.Options{
		MaxRetries:            retries,
		Addr:                  cfg.Addr,
		PoolSize:              cfg.PoolSize,
		DialTimeout:           cfg.DialTimeout,
		ReadTimeout:           cfg.ReadTimeout,
		WriteTimeout:          cfg.WriteTimeout,
		ContextTimeoutEnabled: true,
	})
}

// ConfigFromEnv reads:
//
//	REDIS_ADDR               host:port; empty means "no Redis" (ok=false)
//	REDIS_POOL_SIZE          max connections (default: go-redis, 10 per CPU)
//	REDIS_DIAL_TIMEOUT_MS    default: go-redis, 5000
//	REDIS_READ_TIMEOUT_MS    default: go-redis, 3000
//	REDIS_WRITE_TIMEOUT_MS   default: same as read timeout
//	REDIS_MAX_RETRIES        retries per failed command (default 0, fail fast)
func ConfigFromEnv() (cfg Config, ok bool, err error) {
	cfg.Addr = os.Getenv("REDIS_ADDR")
	if cfg.Addr == "" {
		return Config{}, false, nil
	}
	if cfg.PoolSize, err = envInt("REDIS_POOL_SIZE"); err != nil {
		return Config{}, false, err
	}
	if cfg.DialTimeout, err = envMillis("REDIS_DIAL_TIMEOUT_MS"); err != nil {
		return Config{}, false, err
	}
	if cfg.ReadTimeout, err = envMillis("REDIS_READ_TIMEOUT_MS"); err != nil {
		return Config{}, false, err
	}
	if cfg.WriteTimeout, err = envMillis("REDIS_WRITE_TIMEOUT_MS"); err != nil {
		return Config{}, false, err
	}
	if cfg.MaxRetries, err = envInt("REDIS_MAX_RETRIES"); err != nil {
		return Config{}, false, err
	}
	return cfg, true, nil
}

// FromEnv builds the shared client from the environment. It returns (nil, nil)
// when REDIS_ADDR is unset, so callers fall back to in-memory components.
func FromEnv() (*redis.Client, error) {
	cfg, ok, err := ConfigFromEnv()
	if err != nil || !ok {
		return nil, err
	}
	return New(cfg), nil
}

// Ping checks that Redis answers within timeout.
func Ping(ctx context.Context, c redis.UniversalClient, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

func envInt(k string) (int, error) {
	v := os.Getenv(k)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s=%q: want a non-negative integer", k, v)
	}
	return n, nil
}

func envMillis(k string) (time.Duration, error) {
	n, err := envInt(k)
	return time.Duration(n) * time.Millisecond, err
}
