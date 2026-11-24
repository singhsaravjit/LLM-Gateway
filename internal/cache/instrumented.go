package cache

import (
	"context"
	"strconv"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
)

// Recorder is the part of the metrics registry the cache needs. It matches
// (*metrics.Registry).Inc, so the cache package does not import metrics.
type Recorder interface {
	Inc(name string, v float64)
}

// LookupBuckets are the upper bounds (seconds) of the lookup latency
// histogram. A Redis hit on the same network is about 1 ms; anything past
// the 50 ms lookup timeout is already a miss.
var LookupBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1}

// Instrumented wraps a Cache and records, for every call:
//
//	cache_lookups_total{result="hit"|"miss"|"error"}
//	cache_errors_total{op="get"|"set"}
//	cache_lookup_seconds_bucket{le="..."}, _sum, _count   (Get latency)
//
// The histogram is written as plain counters in Prometheus histogram form,
// so histogram_quantile() works on it without a client library.
type Instrumented struct {
	next Cache
	rec  Recorder
}

// NewInstrumented also writes every series once with 0, so all histogram
// buckets exist from the first scrape and Grafana shows 0, not "no data".
func NewInstrumented(next Cache, rec Recorder) *Instrumented {
	for _, le := range LookupBuckets {
		rec.Inc(bucketName(le), 0)
	}
	for _, n := range []string{
		`cache_lookup_seconds_bucket{le="+Inf"}`, "cache_lookup_seconds_sum", "cache_lookup_seconds_count",
		`cache_lookups_total{result="hit"}`, `cache_lookups_total{result="miss"}`, `cache_lookups_total{result="error"}`,
		`cache_errors_total{op="get"}`, `cache_errors_total{op="set"}`,
	} {
		rec.Inc(n, 0)
	}
	return &Instrumented{next: next, rec: rec}
}

func bucketName(le float64) string {
	return `cache_lookup_seconds_bucket{le="` + strconv.FormatFloat(le, 'g', -1, 64) + `"}`
}

func (c *Instrumented) Get(ctx context.Context, key string) (*api.ChatResponse, bool, error) {
	start := time.Now()
	resp, hit, err := c.next.Get(ctx, key)
	c.observe(time.Since(start).Seconds())
	switch {
	case err != nil:
		c.rec.Inc(`cache_lookups_total{result="error"}`, 1)
		c.rec.Inc(`cache_errors_total{op="get"}`, 1)
	case hit:
		c.rec.Inc(`cache_lookups_total{result="hit"}`, 1)
	default:
		c.rec.Inc(`cache_lookups_total{result="miss"}`, 1)
	}
	return resp, hit, err
}

func (c *Instrumented) Set(ctx context.Context, key string, resp *api.ChatResponse, ttl time.Duration) error {
	err := c.next.Set(ctx, key, resp, ttl)
	if err != nil {
		c.rec.Inc(`cache_errors_total{op="set"}`, 1)
	}
	return err
}

func (c *Instrumented) observe(sec float64) {
	for _, le := range LookupBuckets {
		if sec <= le {
			c.rec.Inc(bucketName(le), 1)
		}
	}
	c.rec.Inc(`cache_lookup_seconds_bucket{le="+Inf"}`, 1)
	c.rec.Inc("cache_lookup_seconds_sum", sec)
	c.rec.Inc("cache_lookup_seconds_count", 1)
}
