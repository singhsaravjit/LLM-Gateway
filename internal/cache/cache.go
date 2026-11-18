// Package cache — owner: Dinesh.
//
// Shared response cache keyed on a hash of (model, prompt). Redis-backed in
// production; the in-memory implementation exists for local dev and tests.
// TODO(dinesh): Redis implementation, TTL config, request coalescing
// (singleflight across nodes), semantic cache (stretch).
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
)

type Cache interface {
	Get(ctx context.Context, key string) (*api.ChatResponse, bool, error)
	Set(ctx context.Context, key string, resp *api.ChatResponse, ttl time.Duration) error
}

// Key builds the cache key. Format is fixed in docs/redis-keys.md: cache:<sha256>.
func Key(model string, msgs []api.Message) string {
	b, _ := json.Marshal(struct {
		M string        `json:"m"`
		P []api.Message `json:"p"`
	}{model, msgs})
	sum := sha256.Sum256(b)
	return "cache:" + hex.EncodeToString(sum[:])
}

type entry struct {
	resp    *api.ChatResponse
	expires time.Time
}

// Memory is a process-local cache. NOT shared across nodes — use for dev only.
type Memory struct {
	mu sync.RWMutex
	m  map[string]entry
}

func NewMemory() *Memory { return &Memory{m: map[string]entry{}} }

func (c *Memory) Get(_ context.Context, key string) (*api.ChatResponse, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.m[key]
	if !ok || time.Now().After(e.expires) {
		return nil, false, nil
	}
	return e.resp, true, nil
}

func (c *Memory) Set(_ context.Context, key string, resp *api.ChatResponse, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = entry{resp: resp, expires: time.Now().Add(ttl)}
	return nil
}
