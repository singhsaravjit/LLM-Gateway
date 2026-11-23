// Package budget — owner: Saravjit.
//
// Per-team monthly spend caps, enforced across all gateway nodes.
// Design: docs/design/queue-and-budgets.md (section 2).
//
// The gateway checks a team's budget before calling the provider and records
// the actual cost after the response. Check and record are separate steps, so
// concurrent in-flight requests can overshoot a limit by at most
// (in-flight requests for the team) x (max cost of one request).
package budget

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Tracker interface {
	// Check reports whether the team may spend more this month. On a backend
	// error it returns the error together with the decision dictated by the
	// fail mode, so callers can count the failure and still act on ok.
	Check(ctx context.Context, team string) (ok bool, err error)
	// Add records spend after a request completes.
	Add(ctx context.Context, team string, usd float64) (Spend, error)
}

// Spend is the result of recording a cost.
type Spend struct {
	TotalUSD float64 // team's spend this month after this request
	LimitUSD float64 // 0 = unlimited
	// CrossedAlert is true for exactly the request that pushed the team past
	// the alert fraction of its limit (e.g. 80%).
	CrossedAlert bool
}

// FailMode decides what Check does when the backend (Redis) is unavailable.
type FailMode int

const (
	FailOpen   FailMode = iota // allow: keep serving, spend may go unchecked
	FailClosed                 // reject: never overspend, service unavailable
)

func ParseFailMode(s string) (FailMode, error) {
	switch s {
	case "", "open":
		return FailOpen, nil
	case "closed":
		return FailClosed, nil
	}
	return 0, fmt.Errorf("unknown budget fail mode %q (want open|closed)", s)
}

// Limits holds monthly caps in USD. A limit <= 0 means unlimited.
type Limits struct {
	Default float64
	PerTeam map[string]float64
	// AlertFraction (0..1) of the limit at which CrossedAlert fires; 0 disables.
	AlertFraction float64
}

func (l Limits) For(team string) float64 {
	if v, ok := l.PerTeam[team]; ok {
		return v
	}
	return l.Default
}

// ParseLimits parses "team-a=50,team-b=12.5" (USD per month).
func ParseLimits(spec string, def, alertFraction float64) (Limits, error) {
	l := Limits{Default: def, PerTeam: map[string]float64{}, AlertFraction: alertFraction}
	if alertFraction < 0 || alertFraction > 1 {
		return l, fmt.Errorf("alert fraction %v not in [0,1]", alertFraction)
	}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		team, val, ok := strings.Cut(part, "=")
		usd, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if !ok || strings.TrimSpace(team) == "" || err != nil {
			return l, fmt.Errorf("bad budget entry %q (want team=usd)", part)
		}
		l.PerTeam[strings.TrimSpace(team)] = usd
	}
	return l, nil
}

// Micro-dollars: spend is stored as an integer so thousands of small
// additions don't accumulate floating-point error.
func toMicros(usd float64) int64  { return int64(math.Round(usd * 1e6)) }
func fromMicros(m int64) float64  { return float64(m) / 1e6 }
func monthKey(t time.Time) string { return t.UTC().Format("2006-01") }

// crossed reports whether going from old to new spend crosses fraction*limit.
func crossed(oldM, newM int64, limit, fraction float64) bool {
	if limit <= 0 || fraction <= 0 {
		return false
	}
	th := toMicros(limit * fraction)
	return oldM < th && newM >= th
}

// Memory is a per-node tracker for local development (not shared across nodes).
type Memory struct {
	limits Limits
	now    func() time.Time
	mu     sync.Mutex
	spent  map[string]int64 // team|month -> micros
}

func NewMemory(l Limits) *Memory {
	return &Memory{limits: l, now: time.Now, spent: map[string]int64{}}
}

func (m *Memory) Check(_ context.Context, team string) (bool, error) {
	limit := m.limits.For(team)
	if limit <= 0 {
		return true, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.spent[team+"|"+monthKey(m.now())] < toMicros(limit), nil
}

func (m *Memory) Add(_ context.Context, team string, usd float64) (Spend, error) {
	limit := m.limits.For(team)
	m.mu.Lock()
	defer m.mu.Unlock()
	k := team + "|" + monthKey(m.now())
	old := m.spent[k]
	m.spent[k] = old + toMicros(usd)
	return Spend{
		TotalUSD:     fromMicros(m.spent[k]),
		LimitUSD:     limit,
		CrossedAlert: crossed(old, m.spent[k], limit, m.limits.AlertFraction),
	}, nil
}
