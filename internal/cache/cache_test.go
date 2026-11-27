package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
)

func TestKeyStable(t *testing.T) {
	m := []api.Message{{Role: "user", Content: "hi"}}
	if Key("cheap", m, 0) != Key("cheap", m, 0) {
		t.Fatal("key not deterministic")
	}
	if Key("cheap", m, 0) == Key("strong", m, 0) {
		t.Fatal("model must be part of key")
	}
}

func TestKeyIncludesMaxTokens(t *testing.T) {
	m := []api.Message{{Role: "user", Content: "hi"}}
	if Key("cheap", m, 50) == Key("cheap", m, 500) {
		t.Fatal("max_tokens must be part of key")
	}
	if Key("cheap", m, 50) == Key("cheap", m, 0) {
		t.Fatal("a limit and no limit must not share a key")
	}
}

// Requests without max_tokens keep the key they had before max_tokens was
// added, so entries already in Redis stay valid after a deploy.
func TestKeyUnchangedWithoutMaxTokens(t *testing.T) {
	m := []api.Message{{Role: "user", Content: "hi"}}
	b, _ := json.Marshal(struct {
		M string        `json:"m"`
		P []api.Message `json:"p"`
	}{"cheap", m})
	sum := sha256.Sum256(b)
	if want := "cache:" + hex.EncodeToString(sum[:]); Key("cheap", m, 0) != want {
		t.Fatalf("key changed for requests without max_tokens: %s, want %s", Key("cheap", m, 0), want)
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
