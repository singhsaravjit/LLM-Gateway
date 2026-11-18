package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestLocalBurstThenRefill(t *testing.T) {
	l := NewLocal(60, 3) // 1 token/s, burst 3
	now := time.Unix(0, 0)
	l.now = func() time.Time { return now }
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if d, _ := l.Allow(ctx, "k"); !d.Allowed {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	if d, _ := l.Allow(ctx, "k"); d.Allowed {
		t.Fatal("4th request should be limited")
	}
	now = now.Add(time.Second)
	if d, _ := l.Allow(ctx, "k"); !d.Allowed {
		t.Fatal("should refill after 1s")
	}
}
