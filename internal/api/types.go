// Package api holds the request/response types shared by the gateway and the
// mock provider. Changes here affect every component: discuss in a PR first.
package api

// ChatRequest is the body clients POST to /v1/chat/completions.
// It is a subset of the OpenAI chat format so real SDKs can talk to us.
type ChatRequest struct {
	Model     string    `json:"model,omitempty"` // "", "auto", "cheap", "strong", or a concrete model
	Messages  []Message `json:"messages"`
	MaxTokens int       `json:"max_tokens,omitempty"`
	// NoCache bypasses the cache for this request (debugging / experiments).
	NoCache bool `json:"no_cache,omitempty"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatResponse struct {
	ID      string   `json:"id"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
}

type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ErrorResponse is returned with every non-2xx status from the gateway.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Type    string `json:"type"` // rate_limited, budget_exceeded, upstream_error, bad_request, queue_full
	Message string `json:"message"`
}

// Headers the gateway reads or sets.
const (
	HeaderAPIKey    = "Authorization" // "Bearer <key>"
	HeaderTeam      = "X-Team-ID"
	HeaderRequestID = "X-Request-ID"
	HeaderCache     = "X-Cache" // HIT | MISS | BYPASS
	HeaderModel     = "X-Model-Used"
	HeaderCostUSD   = "X-Cost-USD"
)
