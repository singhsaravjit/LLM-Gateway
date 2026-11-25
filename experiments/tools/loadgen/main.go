// Command loadgen sends requests at a fixed arrival rate (open loop) to one or
// more gateway nodes, round-robin, and records every request.
//
// Why not Locust for these experiments: Locust users wait for a response
// before sending the next request (closed loop), so when the system slows down
// the offered load silently drops. Experiments 3-5 need a known, constant
// offered load ("50 rps against a 10 rps limit", "2x the provider limit"),
// which an open-loop generator guarantees. Locust (loadtest/) stays the tool
// for the realistic, Zipf-like traffic of Experiments 1 and 7.
//
//	go run ./experiments/tools/loadgen -nodes http://localhost:8081,http://localhost:8082 \
//	    -shape 100:30s -keys 50 -out results/run1
//
// -shape is a list of rate:duration steps, e.g. "150:30s,25:60s" (a burst then calm).
// Output: <out>.csv with one row per request, and a JSON summary on stdout.
package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type step struct {
	rps float64
	dur time.Duration
}

type record struct {
	startMs   int64 // since test start
	node      int
	status    int // 0 = transport error / client timeout
	latencyMs float64
	errType   string
}

type summary struct {
	Sent    int            `json:"sent"`
	Codes   map[string]int `json:"codes"`
	Success float64        `json:"success_pct"`
	P50     float64        `json:"p50_ms"`
	P95     float64        `json:"p95_ms"`
	P99     float64        `json:"p99_ms"`
	WallS   float64        `json:"wall_s"`
}

func parseShape(s string) ([]step, error) {
	var out []step
	for _, part := range strings.Split(s, ",") {
		r, d, ok := strings.Cut(strings.TrimSpace(part), ":")
		rps, err1 := strconv.ParseFloat(r, 64)
		dur, err2 := time.ParseDuration(d)
		if !ok || err1 != nil || err2 != nil || rps < 0 {
			return nil, fmt.Errorf("bad step %q (want rps:duration)", part)
		}
		out = append(out, step{rps, dur})
	}
	return out, nil
}

func main() {
	nodes := flag.String("nodes", "http://localhost:8081", "comma-separated gateway base URLs")
	shape := flag.String("shape", "50:30s", "rate:duration steps")
	keys := flag.Int("keys", 1, "number of distinct API keys (round-robin); 1 = one shared key")
	team := flag.String("team", "loadgen", "X-Team-ID")
	noCache := flag.Bool("nocache", true, "send no_cache so every request takes the full path")
	timeout := flag.Duration("timeout", 60*time.Second, "client timeout per request")
	out := flag.String("out", "", "write per-request CSV to <out>.csv")
	flag.Parse()

	steps, err := parseShape(*shape)
	if err != nil {
		log.Fatal(err)
	}
	urls := strings.Split(*nodes, ",")
	client := &http.Client{Timeout: *timeout, Transport: &http.Transport{MaxIdleConnsPerHost: 1000}}

	var mu sync.Mutex
	var recs []record
	var wg sync.WaitGroup
	start := time.Now()
	next := start
	i := 0
	for _, st := range steps {
		end := next.Add(st.dur)
		if st.rps == 0 {
			next = end
			continue
		}
		gap := time.Duration(float64(time.Second) / st.rps)
		for ; next.Before(end); next = next.Add(gap) {
			time.Sleep(time.Until(next))
			wg.Add(1)
			go func(i int, at time.Time) {
				defer wg.Done()
				node := i % len(urls)
				body := fmt.Sprintf(`{"messages":[{"role":"user","content":"req-%d"}],"no_cache":%v}`, i, *noCache)
				req, _ := http.NewRequest(http.MethodPost, urls[node]+"/v1/chat/completions", bytes.NewBufferString(body))
				req.Header.Set("Authorization", fmt.Sprintf("Bearer key-%d", i%*keys))
				req.Header.Set("X-Team-ID", *team)
				t0 := time.Now()
				resp, err := client.Do(req)
				r := record{startMs: at.Sub(start).Milliseconds(), node: node}
				if err == nil {
					r.status = resp.StatusCode
					if resp.StatusCode != 200 {
						var e struct {
							Error struct{ Type string } `json:"error"`
						}
						_ = json.NewDecoder(resp.Body).Decode(&e)
						r.errType = e.Error.Type
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				} else {
					r.errType = "client_error"
				}
				r.latencyMs = float64(time.Since(t0).Microseconds()) / 1000
				mu.Lock()
				recs = append(recs, r)
				mu.Unlock()
			}(i, next)
			i++
		}
	}
	wg.Wait()

	sort.Slice(recs, func(a, b int) bool { return recs[a].startMs < recs[b].startMs })
	s := summary{Sent: len(recs), Codes: map[string]int{}, WallS: time.Since(start).Seconds()}
	var lat []float64
	for _, r := range recs {
		s.Codes[strconv.Itoa(r.status)]++
		if r.status == 200 {
			lat = append(lat, r.latencyMs)
		}
	}
	sort.Float64s(lat)
	pct := func(q float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(q*float64(len(lat)-1))]
	}
	if s.Sent > 0 {
		s.Success = 100 * float64(s.Codes["200"]) / float64(s.Sent)
	}
	s.P50, s.P95, s.P99 = pct(0.50), pct(0.95), pct(0.99)

	if *out != "" {
		f, err := os.Create(*out + ".csv")
		if err != nil {
			log.Fatal(err)
		}
		w := csv.NewWriter(f)
		_ = w.Write([]string{"start_ms", "node", "status", "latency_ms", "error_type"})
		for _, r := range recs {
			_ = w.Write([]string{strconv.FormatInt(r.startMs, 10), strconv.Itoa(r.node), strconv.Itoa(r.status),
				strconv.FormatFloat(r.latencyMs, 'f', 2, 64), r.errType})
		}
		w.Flush()
		f.Close()
	}
	_ = json.NewEncoder(os.Stdout).Encode(s)
}
