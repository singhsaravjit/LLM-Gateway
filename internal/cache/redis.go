package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/singhsaravjit/llm-gateway/internal/api"
)

// Redis is the shared cache: every gateway node reads and writes the same
// entries, so a response cached through node A is a hit on node B. Entries are
// JSON ChatResponses stored under cache:<sha256> (see Key) with a Redis TTL.
//
// It fails open. A slow or unreachable Redis, or an entry that cannot be
// decoded, is reported as a miss together with the error: the caller counts
// the error and calls the provider, so the cache can never fail a request.
type Redis struct {
	client  redis.Cmdable
	timeout time.Duration // per-call budget; a slow Redis turns into a miss
}

type RedisOption func(*Redis)

// WithTimeout sets the per-call Redis budget (default 50 ms). 0 disables it.
func WithTimeout(d time.Duration) RedisOption { return func(r *Redis) { r.timeout = d } }

// NewRedis builds the cache on a go-redis client (normally the shared one from
// internal/redisx, which honours context deadlines).
func NewRedis(client redis.Cmdable, opts ...RedisOption) *Redis {
	r := &Redis{client: client, timeout: 50 * time.Millisecond}
	for _, o := range opts {
		o(r)
	}
	return r
}

func (r *Redis) Get(ctx context.Context, key string) (*api.ChatResponse, bool, error) {
	ctx, cancel := r.withTimeout(ctx)
	defer cancel()
	b, err := r.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("cache redis get: %w", err)
	}
	var resp api.ChatResponse
	if err := json.Unmarshal(b, &resp); err != nil {
		return nil, false, fmt.Errorf("cache redis decode %s: %w", key, err)
	}
	return &resp, true, nil
}

func (r *Redis) Set(ctx context.Context, key string, resp *api.ChatResponse, ttl time.Duration) error {
	if resp == nil {
		return errors.New("cache redis set: nil response")
	}
	// TTL 0 means "never expire" to Redis but "already expired" to Memory.
	// Match Memory: nothing is stored, so CACHE_TTL_SECONDS=0 never caches.
	if ttl <= 0 {
		return nil
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("cache redis encode: %w", err)
	}
	ctx, cancel := r.withTimeout(ctx)
	defer cancel()
	if err := r.client.Set(ctx, key, b, ttl).Err(); err != nil {
		return fmt.Errorf("cache redis set: %w", err)
	}
	return nil
}

func (r *Redis) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if r.timeout > 0 {
		return context.WithTimeout(ctx, r.timeout)
	}
	return ctx, func() {}
}
