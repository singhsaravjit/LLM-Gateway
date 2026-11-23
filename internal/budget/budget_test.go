package budget

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/redisx/redistest"
)

func TestParseLimits(t *testing.T) {
	l, err := ParseLimits(" team-a=50, team-b = 12.5 ,", 10, 0.8)
	if err != nil {
		t.Fatal(err)
	}
	if l.For("team-a") != 50 || l.For("team-b") != 12.5 || l.For("other") != 10 {
		t.Fatalf("limits = %+v", l)
	}
	for _, bad := range []string{"team-a", "=5", "team-a=lots"} {
		if _, err := ParseLimits(bad, 0, 0); err == nil {
			t.Errorf("ParseLimits(%q) should fail", bad)
		}
	}
	if _, err := ParseLimits("", 0, 1.5); err == nil {
		t.Error("alert fraction > 1 should fail")
	}
}

// Both implementations must behave the same way.
func trackers(t *testing.T, l Limits, now func() time.Time) map[string]Tracker {
	_, c := redistest.Start(t)
	m := NewMemory(l)
	m.now = now
	r := NewRedis(c, l)
	r.now = now
	return map[string]Tracker{"memory": m, "redis": r}
}

func fixed(s string) func() time.Time {
	ts, _ := time.Parse(time.RFC3339, s)
	return func() time.Time { return ts }
}

func TestLimitEnforcedAndAlert(t *testing.T) {
	l := Limits{Default: 1.0, AlertFraction: 0.8}
	for name, tr := range trackers(t, l, fixed("2026-11-23T12:00:00Z")) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var alerts int
			for i := 0; i < 10; i++ { // 10 x $0.10 = $1.00
				if ok, err := tr.Check(ctx, "t"); err != nil || !ok {
					t.Fatalf("request %d: ok=%v err=%v", i, ok, err)
				}
				s, err := tr.Add(ctx, "t", 0.10)
				if err != nil {
					t.Fatal(err)
				}
				if s.CrossedAlert {
					alerts++
					if math.Abs(s.TotalUSD-0.80) > 1e-9 {
						t.Errorf("alert fired at $%.2f, want $0.80", s.TotalUSD)
					}
				}
			}
			if alerts != 1 {
				t.Errorf("alerts = %d, want exactly 1", alerts)
			}
			if ok, _ := tr.Check(ctx, "t"); ok {
				t.Error("team at $1.00 of $1.00 should be blocked")
			}
			if ok, _ := tr.Check(ctx, "someone-else"); !ok {
				t.Error("other teams have their own budget")
			}
		})
	}
}

func TestUnlimitedAndPerTeam(t *testing.T) {
	l := Limits{PerTeam: map[string]float64{"capped": 0.05}}
	for name, tr := range trackers(t, l, fixed("2026-11-23T12:00:00Z")) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			_, _ = tr.Add(ctx, "free", 1000)
			if ok, _ := tr.Check(ctx, "free"); !ok {
				t.Error("default 0 means unlimited")
			}
			_, _ = tr.Add(ctx, "capped", 0.05)
			if ok, _ := tr.Check(ctx, "capped"); ok {
				t.Error("capped team should be blocked")
			}
		})
	}
}

func TestNewMonthResets(t *testing.T) {
	now := fixed("2026-11-30T23:59:59Z")
	l := Limits{Default: 1}
	for name, tr := range trackers(t, l, func() time.Time { return now() }) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			_, _ = tr.Add(ctx, "t", 1)
			if ok, _ := tr.Check(ctx, "t"); ok {
				t.Fatal("should be blocked in November")
			}
			switch v := tr.(type) {
			case *Memory:
				v.now = fixed("2026-12-01T00:00:00Z")
			case *Redis:
				v.now = fixed("2026-12-01T00:00:00Z")
			}
			if ok, _ := tr.Check(ctx, "t"); !ok {
				t.Fatal("new month should start at $0")
			}
		})
	}
}

func TestMicrosNoFloatDrift(t *testing.T) {
	_, c := redistest.Start(t)
	r := NewRedis(c, Limits{})
	ctx := context.Background()
	var s Spend
	for i := 0; i < 10000; i++ {
		s, _ = r.Add(ctx, "t", 0.0001) // 10,000 x $0.0001 = exactly $1
	}
	if s.TotalUSD != 1.0 {
		t.Fatalf("total = %.10f, want exactly 1", s.TotalUSD)
	}
}

func TestRedisSharedAcrossNodesAndKeyFormat(t *testing.T) {
	mr, c := redistest.Start(t)
	l := Limits{Default: 100}
	nodes := make([]*Redis, 4)
	for i := range nodes {
		nodes[i] = NewRedis(c, l)
		nodes[i].now = fixed("2026-11-23T12:00:00Z")
	}
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				_, _ = n.Add(context.Background(), "t", 0.01)
			}
		}()
	}
	wg.Wait()
	got, err := mr.Get("budget:t:2026-11")
	if err != nil || got != "10000000" { // 1,000 x $0.01 = $10 = 10,000,000 micros
		t.Fatalf("budget:t:2026-11 = %q (%v), want 10000000", got, err)
	}
	if ttl := mr.TTL("budget:t:2026-11"); ttl <= 0 {
		t.Fatalf("key should expire, ttl=%v", ttl)
	}
}

func TestRedisFailModes(t *testing.T) {
	mr, _ := redistest.Start(t)
	p := redistest.NewFaultProxy(t, mr.Addr())
	c := redistest.NewClient(t, p.Addr())
	l := Limits{Default: 1}
	ctx := context.Background()

	p.Down()
	if ok, err := NewRedis(c, l, WithFailMode(FailOpen)).Check(ctx, "t"); err == nil || !ok {
		t.Errorf("fail-open: ok=%v err=%v, want true + error", ok, err)
	}
	if ok, err := NewRedis(c, l, WithFailMode(FailClosed)).Check(ctx, "t"); err == nil || ok {
		t.Errorf("fail-closed: ok=%v err=%v, want false + error", ok, err)
	}
	if _, err := NewRedis(c, l).Add(ctx, "t", 0.5); err == nil {
		t.Error("Add should report the error so lost spend can be logged")
	}

	// Slow Redis: the call gives up at its deadline instead of hanging.
	p.Up()
	p.SetDelay(500 * time.Millisecond)
	start := time.Now()
	_, err := NewRedis(c, l, WithTimeout(50*time.Millisecond)).Check(ctx, "t")
	if err == nil || time.Since(start) > 300*time.Millisecond {
		t.Errorf("slow redis: err=%v after %v, want timeout error well under 500ms", err, time.Since(start))
	}
}
