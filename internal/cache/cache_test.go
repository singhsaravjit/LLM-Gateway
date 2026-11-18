package cache

import (
	"context"
	"testing"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
)

func TestKeyStable(t *testing.T) {
	m := []api.Message{{Role: "user", Content: "hi"}}
	if Key("cheap", m) != Key("cheap", m) {
		t.Fatal("key not deterministic")
	}
	if Key("cheap", m) == Key("strong", m) {
		t.Fatal("model must be part of key")
	}
}

func TestMemoryTTL(t *testing.T) {
	c := NewMemory()
	ctx := context.Background()
	_ = c.Set(ctx, "k", &api.ChatResponse{ID: "1"}, 20*time.Millisecond)
	if _, ok, _ := c.Get(ctx, "k"); !ok {
		t.Fatal("expected hit")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok, _ := c.Get(ctx, "k"); ok {
		t.Fatal("expected expiry")
	}
}
