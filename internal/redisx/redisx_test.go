package redisx

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestFromEnvNoAddr(t *testing.T) {
	t.Setenv("REDIS_ADDR", "")
	c, err := FromEnv()
	if err != nil || c != nil {
		t.Fatalf("want (nil, nil) without REDIS_ADDR, got (%v, %v)", c, err)
	}
}

func TestFromEnvAndPing(t *testing.T) {
	mr := miniredis.RunT(t)
	t.Setenv("REDIS_ADDR", mr.Addr())
	t.Setenv("REDIS_POOL_SIZE", "7")
	t.Setenv("REDIS_READ_TIMEOUT_MS", "250")

	c, err := FromEnv()
	if err != nil || c == nil {
		t.Fatalf("FromEnv: client=%v err=%v", c, err)
	}
	t.Cleanup(func() { c.Close() })

	o := c.Options()
	if o.Addr != mr.Addr() || o.PoolSize != 7 || o.ReadTimeout != 250*time.Millisecond || !o.ContextTimeoutEnabled {
		t.Fatalf("options not applied: addr=%s pool=%d read=%v ctxTimeout=%v",
			o.Addr, o.PoolSize, o.ReadTimeout, o.ContextTimeoutEnabled)
	}
	if err := Ping(context.Background(), c, time.Second); err != nil {
		t.Fatalf("ping: %v", err)
	}
	// The client is usable for normal commands.
	if err := c.Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got, _ := mr.Get("k"); got != "v" {
		t.Fatalf("got %q, want v", got)
	}
}

func TestFromEnvBadValue(t *testing.T) {
	t.Setenv("REDIS_ADDR", "localhost:6379")
	for _, k := range []string{"REDIS_POOL_SIZE", "REDIS_DIAL_TIMEOUT_MS", "REDIS_READ_TIMEOUT_MS", "REDIS_WRITE_TIMEOUT_MS"} {
		t.Run(k, func(t *testing.T) {
			t.Setenv(k, "abc")
			if c, err := FromEnv(); err == nil {
				c.Close()
				t.Fatalf("%s=abc: want error", k)
			}
		})
	}
}

func TestPingRedisDown(t *testing.T) {
	mr := miniredis.RunT(t)
	c := New(Config{Addr: mr.Addr()})
	t.Cleanup(func() { c.Close() })
	mr.Close()
	if err := Ping(context.Background(), c, 200*time.Millisecond); err == nil {
		t.Fatal("want error when Redis is down")
	}
}

// A server that accepts connections but never replies stands in for a hung
// Redis. The call must give up at the context deadline, not after the 3 s
// default read timeout; this is what ContextTimeoutEnabled buys us.
func TestContextDeadlineIsHonoured(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()

	c := New(Config{Addr: ln.Addr().String()})
	t.Cleanup(func() { c.Close() })

	start := time.Now()
	err = Ping(context.Background(), c, 100*time.Millisecond)
	took := time.Since(start)
	if err == nil {
		t.Fatal("want timeout error from a server that never replies")
	}
	if took > time.Second {
		t.Fatalf("ping took %v; context deadline (100ms) was ignored", took)
	}
}
