package redistest

import (
	"context"
	"testing"
	"time"
)

func TestStart(t *testing.T) {
	mr, c := Start(t)
	ctx := context.Background()
	if err := c.Set(ctx, "k", "v", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if got, _ := mr.Get("k"); got != "v" {
		t.Fatalf("miniredis has %q, want v", got)
	}
	if !c.Options().ContextTimeoutEnabled {
		t.Fatal("client should use redisx settings (ContextTimeoutEnabled)")
	}
	mr.FastForward(2 * time.Minute)
	if mr.Exists("k") {
		t.Fatal("key should expire after FastForward")
	}
}

func TestProxyPassesTraffic(t *testing.T) {
	mr, _ := Start(t)
	p := NewFaultProxy(t, mr.Addr())
	c := NewClient(t, p.Addr())
	ctx := context.Background()
	if err := c.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Get(ctx, "k").Result(); err != nil || got != "v" {
		t.Fatalf("get through proxy = %q, %v", got, err)
	}
}

func TestProxyDelay(t *testing.T) {
	mr, _ := Start(t)
	p := NewFaultProxy(t, mr.Addr())
	c := NewClient(t, p.Addr())
	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil { // open the connection first
		t.Fatal(err)
	}

	p.SetDelay(150 * time.Millisecond)
	start := time.Now()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 150*time.Millisecond {
		t.Fatalf("ping took %v, want >= 150ms with delay", took)
	}

	// A caller with a tighter deadline gives up at its deadline.
	tctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	start = time.Now()
	if err := c.Ping(tctx).Err(); err == nil {
		t.Fatal("want timeout error from slow Redis")
	}
	if took := time.Since(start); took > 120*time.Millisecond {
		t.Fatalf("timed-out ping took %v, want about 30ms", took)
	}

	p.SetDelay(0)
	start = time.Now()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 100*time.Millisecond {
		t.Fatalf("ping took %v after removing delay", took)
	}
}

func TestProxyDownAndUp(t *testing.T) {
	mr, _ := Start(t)
	p := NewFaultProxy(t, mr.Addr())
	c := NewClient(t, p.Addr())
	ctx := context.Background()
	if err := c.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}

	p.Down()
	if err := c.Get(ctx, "k").Err(); err == nil {
		t.Fatal("want error while proxy is down")
	}

	p.Up()
	if got, err := c.Get(ctx, "k").Result(); err != nil || got != "v" {
		t.Fatalf("after Up: got %q, %v; data should still be there", got, err)
	}
}
