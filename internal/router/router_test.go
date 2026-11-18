package router

import (
	"strings"
	"testing"

	"github.com/singhsaravjit/llm-gateway/internal/api"
)

func TestLengthRouter(t *testing.T) {
	r := LengthRouter{MaxCheapChars: 10}
	short := &api.ChatRequest{Messages: []api.Message{{Content: "hi"}}}
	long := &api.ChatRequest{Messages: []api.Message{{Content: strings.Repeat("x", 50)}}}
	if got := r.Route(short); got != Cheap {
		t.Errorf("short: got %s", got)
	}
	if got := r.Route(long); got != Strong {
		t.Errorf("long: got %s", got)
	}
	if got := r.Route(&api.ChatRequest{Model: Strong, Messages: short.Messages}); got != Strong {
		t.Errorf("explicit: got %s", got)
	}
}
