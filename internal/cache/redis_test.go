package cache

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
	"github.com/singhsaravjit/llm-gateway/internal/redisx/redistest"
)

var _ Cache = (*Redis)(nil)

func sampleResp() *api.ChatResponse {
	return &api.ChatResponse{
		ID:    "resp-1",
		Model: "cheap",
		Choices: []api.Choice{{
			Index:        0,
			Message:      api.Message{Role: "assistant", Content: "hello there"},
			FinishReason: "stop",
		}},
		Usage: api.Usage{PromptTokens: 12, CompletionTokens: 30, TotalTokens: 42},
	}
}

func testKey() string {
	return Key("cheap", []api.Message{{Role: "user", Content: "hi"}})
}

func TestRedisSetGetRoundTrip(t *testing.T) {
	mr, c := redistest.Start(t)
	rc := NewRedis(c)
	ctx := context.Background()
	key, want := testKey(), sampleResp()

	if err := rc.Set(ctx, key, want, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, hit, err := rc.Get(ctx, key)
	if err != nil || !hit {
		t.Fatalf("get: hit=%v err=%v", hit, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	// Stored under the documented key format with the TTL applied.
	if !mr.Exists(key) || mr.TTL(key) != time.Minute {
		t.Fatalf("key %s: exists=%v ttl=%v", key, mr.Exists(key), mr.TTL(key))
	}
}

func TestRedisMiss(t *testing.T) {
	_, c := redistest.Start(t)
	got, hit, err := NewRedis(c).Get(context.Background(), "cache:unknown")
	if got != nil || hit || err != nil {
		t.Fatalf("miss: got=%v hit=%v err=%v, want nil,false,nil", got, hit, err)
	}
}

func TestRedisTTLExpiry(t *testing.T) {
	mr, c := redistest.Start(t)
	rc := NewRedis(c)
	ctx := context.Background()
	key := testKey()
	if err := rc.Set(ctx, key, sampleResp(), 30*time.Second); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(29 * time.Second)
	if _, hit, _ := rc.Get(ctx, key); !hit {
		t.Fatal("want hit before TTL")
	}
	mr.FastForward(2 * time.Second)
	if _, hit, err := rc.Get(ctx, key); hit || err != nil {
		t.Fatalf("want clean miss after TTL, hit=%v err=%v", hit, err)
	}
}

func TestRedisZeroTTLNotStored(t *testing.T) {
	mr, c := redistest.Start(t)
	rc := NewRedis(c)
	key := testKey()
	if err := rc.Set(context.Background(), key, sampleResp(), 0); err != nil {
		t.Fatal(err)
	}
	if mr.Exists(key) {
		t.Fatal("ttl 0 must not store (would never expire in Redis)")
	}
}

// The property #4 is about: two gateway nodes, one Redis, one cache.
func TestRedisSharedAcrossNodes(t *testing.T) {
	mr, _ := redistest.Start(t)
	nodeA := NewRedis(redistest.NewClient(t, mr.Addr()))
	nodeB := NewRedis(redistest.NewClient(t, mr.Addr()))
	ctx := context.Background()
	key := testKey()

	if err := nodeA.Set(ctx, key, sampleResp(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if got, hit, err := nodeB.Get(ctx, key); err != nil || !hit || got.ID != "resp-1" {
		t.Fatalf("node B: hit=%v err=%v got=%+v", hit, err, got)
	}
	// Memory caches do not share: the contrast Experiment 1 measures.
	memA, memB := NewMemory(), NewMemory()
	_ = memA.Set(ctx, key, sampleResp(), time.Minute)
	if _, hit, _ := memB.Get(ctx, key); hit {
		t.Fatal("separate Memory caches should not share entries")
	}
}

func TestRedisDownIsMissWithError(t *testing.T) {
	mr, _ := redistest.Start(t)
	p := redistest.NewFaultProxy(t, mr.Addr())
	rc := NewRedis(redistest.NewClient(t, p.Addr()))
	ctx := context.Background()
	key := testKey()
	if err := rc.Set(ctx, key, sampleResp(), time.Minute); err != nil {
		t.Fatal(err)
	}

	p.Down()
	got, hit, err := rc.Get(ctx, key)
	if got != nil || hit || err == nil {
		t.Fatalf("down: got=%v hit=%v err=%v, want nil,false,error", got, hit, err)
	}
	if err := rc.Set(ctx, key, sampleResp(), time.Minute); err == nil {
		t.Fatal("set while down: want error")
	}

	p.Up() // recovers, entry still there
	if _, hit, err := rc.Get(ctx, key); !hit || err != nil {
		t.Fatalf("after recovery: hit=%v err=%v", hit, err)
	}
}

func TestRedisSlowIsMissAfterTimeout(t *testing.T) {
	mr, _ := redistest.Start(t)
	p := redistest.NewFaultProxy(t, mr.Addr())
	rc := NewRedis(redistest.NewClient(t, p.Addr()), WithTimeout(50*time.Millisecond))
	ctx := context.Background()
	key := testKey()
	if err := rc.Set(ctx, key, sampleResp(), time.Minute); err != nil {
		t.Fatal(err)
	}

	p.SetDelay(300 * time.Millisecond)
	start := time.Now()
	_, hit, err := rc.Get(ctx, key)
	took := time.Since(start)
	if hit || err == nil {
		t.Fatalf("slow: hit=%v err=%v, want miss with timeout error", hit, err)
	}
	if took > 200*time.Millisecond {
		t.Fatalf("slow get took %v; should give up near the 50ms timeout", took)
	}
}

func TestRedisCorruptEntryIsMissWithError(t *testing.T) {
	mr, c := redistest.Start(t)
	key := testKey()
	if err := mr.Set(key, "{not json"); err != nil {
		t.Fatal(err)
	}
	got, hit, err := NewRedis(c).Get(context.Background(), key)
	if got != nil || hit || err == nil {
		t.Fatalf("corrupt: got=%v hit=%v err=%v, want nil,false,error", got, hit, err)
	}
}

func TestRedisSetNil(t *testing.T) {
	_, c := redistest.Start(t)
	if err := NewRedis(c).Set(context.Background(), testKey(), nil, time.Minute); err == nil {
		t.Fatal("want error for nil response")
	}
}
