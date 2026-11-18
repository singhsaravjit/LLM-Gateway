// Package metrics — owner: Kanad.
//
// Minimal Prometheus text exposition with no external deps so the skeleton
// builds anywhere. TODO(kanad): switch to prometheus/client_golang with
// latency histograms (p50/p95/p99), queue depth gauge, etc.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

type Registry struct {
	mu       sync.Mutex
	counters map[string]float64
}

func New() *Registry { return &Registry{counters: map[string]float64{}} }

// Inc adds v to a counter. name may include labels, e.g. `requests_total{cache="hit"}`.
func (r *Registry) Inc(name string, v float64) {
	r.mu.Lock()
	r.counters[name] += v
	r.mu.Unlock()
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.mu.Lock()
		keys := make([]string, 0, len(r.counters))
		for k := range r.counters {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&b, "llmgw_%s %g\n", k, r.counters[k])
		}
		r.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(b.String()))
	})
}
