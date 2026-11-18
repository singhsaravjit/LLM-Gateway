// Package provider — owner: Preshit.
//
// HTTP client for LLM providers (the mock, or a real API for the demo).
// TODO(preshit): retries with exponential backoff + jitter, circuit breaker,
// fallback to the other model, real-provider adapter for the demo.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
)

// StatusError is returned when the provider answers with a non-2xx status.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string { return fmt.Sprintf("provider status %d: %s", e.Code, e.Body) }

type Client interface {
	Complete(ctx context.Context, model string, req *api.ChatRequest) (*api.ChatResponse, error)
}

// HTTP calls an OpenAI-compatible /v1/chat/completions endpoint.
type HTTP struct {
	BaseURL string
	APIKey  string
	Client  *http.Client
}

func NewHTTP(baseURL, apiKey string, timeout time.Duration) *HTTP {
	return &HTTP{BaseURL: baseURL, APIKey: apiKey, Client: &http.Client{Timeout: timeout}}
}

func (h *HTTP) Complete(ctx context.Context, model string, req *api.ChatRequest) (*api.ChatResponse, error) {
	out := *req
	out.Model = model
	body, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, h.BaseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	hr.Header.Set("Content-Type", "application/json")
	if h.APIKey != "" {
		hr.Header.Set("Authorization", "Bearer "+h.APIKey)
	}
	resp, err := h.Client.Do(hr)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(resp.Body)
		return nil, &StatusError{Code: resp.StatusCode, Body: buf.String()}
	}
	var cr api.ChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return nil, err
	}
	return &cr, nil
}

// Pricing in USD per 1K tokens. Keep in sync with cmd/mockllm.
var Pricing = map[string]struct{ In, Out float64 }{
	"cheap":  {In: 0.00015, Out: 0.0006},
	"strong": {In: 0.0025, Out: 0.01},
}

func Cost(model string, u api.Usage) float64 {
	p, ok := Pricing[model]
	if !ok {
		return 0
	}
	return float64(u.PromptTokens)/1000*p.In + float64(u.CompletionTokens)/1000*p.Out
}
