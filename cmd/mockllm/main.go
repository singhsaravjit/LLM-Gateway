// Command mockllm imitates an LLM provider (owner: Kanad) so load tests cost $0.
//
// Configurable via env:
//
//	LATENCY_MS_MEAN / LATENCY_MS_JITTER  simulated response time
//	RATE_LIMIT_RPS                       provider-side limit; excess gets 429 (0 = off)
//	ERROR_RATE                           fraction of requests that return 500
//	STRONG_LATENCY_MULT                  "strong" model is this many times slower
//
// TODO(kanad): per-model error injection (Exp. 6), tokens-per-minute limits,
// calibrate defaults against a few real provider calls.
package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
)

type cfg struct {
	latencyMean, latencyJitter time.Duration
	rps                        float64
	errorRate, strongMult      float64
}

type limiter struct {
	mu     sync.Mutex
	rps    float64
	tokens float64
	last   time.Time
}

func (l *limiter) allow() bool {
	if l.rps <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.tokens = min(l.rps, l.tokens+now.Sub(l.last).Seconds()*l.rps)
	l.last = now
	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}

func main() {
	c := cfg{
		latencyMean:   time.Duration(envF("LATENCY_MS_MEAN", 800)) * time.Millisecond,
		latencyJitter: time.Duration(envF("LATENCY_MS_JITTER", 300)) * time.Millisecond,
		rps:           envF("RATE_LIMIT_RPS", 50),
		errorRate:     envF("ERROR_RATE", 0.0),
		strongMult:    envF("STRONG_LATENCY_MULT", 2.5),
	}
	lim := &limiter{rps: c.rps, tokens: c.rps, last: time.Now()}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var req api.ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if !lim.allow() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, `{"error":{"type":"rate_limited","message":"mock provider limit"}}`, http.StatusTooManyRequests)
			return
		}
		lat := c.latencyMean + time.Duration((rand.Float64()*2-1)*float64(c.latencyJitter))
		if req.Model == "strong" {
			lat = time.Duration(float64(lat) * c.strongMult)
		}
		select {
		case <-time.After(max(lat, 0)):
		case <-r.Context().Done():
			return
		}
		if rand.Float64() < c.errorRate {
			http.Error(w, `{"error":{"type":"server_error","message":"injected failure"}}`, http.StatusInternalServerError)
			return
		}
		prompt := 0
		for _, m := range req.Messages {
			prompt += len(m.Content)/4 + 1 // ~4 chars per token
		}
		completion := 50 + rand.IntN(200)
		if req.MaxTokens > 0 {
			completion = min(completion, req.MaxTokens)
		}
		resp := api.ChatResponse{
			ID:    fmt.Sprintf("mock-%d", rand.Int64()),
			Model: req.Model,
			Choices: []api.Choice{{
				Message:      api.Message{Role: "assistant", Content: fmt.Sprintf("[mock %s reply, %d tokens]", req.Model, completion)},
				FinishReason: "stop",
			}},
			Usage: api.Usage{PromptTokens: prompt, CompletionTokens: completion, TotalTokens: prompt + completion},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	addr := ":" + envS("PORT", "9000")
	log.Info("mock LLM listening", "addr", addr, "rps_limit", c.rps, "latency_mean", c.latencyMean.String())
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("stopped", "err", err)
		os.Exit(1)
	}
}

func envS(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envF(k string, d float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
		return v
	}
	return d
}
