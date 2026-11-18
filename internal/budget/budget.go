// Package budget — owner: Saravjit.
//
// Per-team spend counters, shared across nodes via Redis (INCRBYFLOAT).
// TODO(saravjit): Redis implementation, monthly budgets + alerts (stretch).
package budget

import (
	"context"
	"sync"
)

type Tracker interface {
	// Check reports whether the team is still under its budget.
	Check(ctx context.Context, team string) (bool, error)
	// Add records spend after a request completes.
	Add(ctx context.Context, team string, usd float64) error
}

// Memory is a per-node tracker for local development.
type Memory struct {
	mu    sync.Mutex
	limit float64 // USD; <=0 means unlimited
	spent map[string]float64
}

func NewMemory(limitUSD float64) *Memory {
	return &Memory{limit: limitUSD, spent: map[string]float64{}}
}

func (m *Memory) Check(_ context.Context, team string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.limit <= 0 || m.spent[team] < m.limit, nil
}

func (m *Memory) Add(_ context.Context, team string, usd float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spent[team] += usd
	return nil
}
