// Package router — owner: Preshit.
//
// Picks a model for each request. MVP is rule-based (prompt length or an
// explicit request flag). TODO(preshit): configurable rules, routing-share
// knob for Experiment 6.
package router

import "github.com/singhsaravjit/llm-gateway/internal/api"

const (
	Cheap  = "cheap"
	Strong = "strong"
)

type Router interface {
	Route(req *api.ChatRequest) string
}

// LengthRouter sends short prompts to the cheap model and long ones to the strong model.
type LengthRouter struct {
	MaxCheapChars int
}

func (r LengthRouter) Route(req *api.ChatRequest) string {
	switch req.Model {
	case Cheap, Strong:
		return req.Model
	case "", "auto":
	default:
		return req.Model // explicit concrete model
	}
	n := 0
	for _, m := range req.Messages {
		n += len(m.Content)
	}
	if n <= r.MaxCheapChars {
		return Cheap
	}
	return Strong
}
