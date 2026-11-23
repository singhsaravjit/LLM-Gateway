package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
)

type fakeRec map[string]float64

func (f fakeRec) Inc(name string, v float64) { f[name] += v }

type failingCache struct{}

func (failingCache) Get(context.Context, string) (*api.ChatResponse, bool, error) {
	return nil, false, errors.New("redis down")
}
func (failingCache) Set(context.Context, string, *api.ChatResponse, time.Duration) error {
	return errors.New("redis down")
}

func TestInstrumentedCountsHitMiss(t *testing.T) {
	rec := fakeRec{}
	c := NewInstrumented(NewMemory(), rec)
	ctx := context.Background()

	if _, hit, _ := c.Get(ctx, "k"); hit {
		t.Fatal("expected miss")
	}
	if err := c.Set(ctx, "k", &api.ChatResponse{ID: "1"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, hit, _ := c.Get(ctx, "k"); !hit {
			t.Fatal("expected hit")
		}
	}
	want := map[string]float64{
		`cache_lookups_total{result="hit"}`:      2,
		`cache_lookups_total{result="miss"}`:     1,
		`cache_lookups_total{result="error"}`:    0,
		`cache_errors_total{op="get"}`:           0,
		`cache_errors_total{op="set"}`:           0,
		"cache_lookup_seconds_count":             3,
		`cache_lookup_seconds_bucket{le="+Inf"}`: 3,
	}
	for k, v := range want {
		if got, ok := rec[k]; !ok || got != v {
			t.Errorf("%s = %v (present %v), want %v", k, got, ok, v)
		}
	}
	// Memory lookups take microseconds, so every bucket holds all 3.
	for _, le := range LookupBuckets {
		if got := rec[bucketName(le)]; got != 3 {
			t.Errorf("%s = %v, want 3", bucketName(le), got)
		}
	}
}

func TestInstrumentedCountsErrors(t *testing.T) {
	rec := fakeRec{}
	c := NewInstrumented(failingCache{}, rec)
	ctx := context.Background()

	if _, hit, err := c.Get(ctx, "k"); hit || err == nil {
		t.Fatalf("want miss with error, got hit=%v err=%v", hit, err)
	}
	if err := c.Set(ctx, "k", &api.ChatResponse{}, time.Minute); err == nil {
		t.Fatal("want set error passed through")
	}
	for k, v := range map[string]float64{
		`cache_lookups_total{result="error"}`: 1,
		`cache_lookups_total{result="miss"}`:  0,
		`cache_errors_total{op="get"}`:        1,
		`cache_errors_total{op="set"}`:        1,
	} {
		if rec[k] != v {
			t.Errorf("%s = %v, want %v", k, rec[k], v)
		}
	}
}

func TestInstrumentedBuckets(t *testing.T) {
	rec := fakeRec{}
	c := NewInstrumented(NewMemory(), rec)
	c.observe(0.003) // 3 ms: above 0.0025, inside 0.005
	for _, le := range LookupBuckets {
		want := 0.0
		if le >= 0.005 {
			want = 1
		}
		if got := rec[bucketName(le)]; got != want {
			t.Errorf("%s = %v, want %v", bucketName(le), got, want)
		}
	}
	if rec["cache_lookup_seconds_sum"] != 0.003 {
		t.Errorf("sum = %v", rec["cache_lookup_seconds_sum"])
	}
}
