package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/singhsaravjit/llm-gateway/internal/api"
)

// Memory is an in-process queue: jobs wait in a channel on the node that
// received them. The provider rate can still be shared across nodes by giving
// it a Redis-backed Pacer.
type Memory struct {
	cfg    Config
	jobs   chan *memJob
	depth  atomic.Int64
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type memJob struct {
	model    string
	req      *api.ChatRequest
	deadline time.Time
	done     chan result // buffered(1): workers never block on a gone client
}

type result struct {
	resp *api.ChatResponse
	err  error
}

func NewMemory(cfg Config) *Memory {
	cfg.defaults()
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	m := &Memory{cfg: cfg, jobs: make(chan *memJob, cfg.Max)}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	for i := 0; i < cfg.Workers; i++ {
		m.wg.Add(1)
		go m.worker()
	}
	return m
}

func (m *Memory) Submit(ctx context.Context, model string, req *api.ChatRequest) (*api.ChatResponse, error) {
	if m.depth.Add(1) > m.cfg.Max {
		m.depth.Add(-1)
		m.cfg.Obs.Inc(`queue_rejected_total{reason="full"}`, 1)
		return nil, ErrFull
	}
	j := &memJob{model: model, req: req, deadline: deadlineFor(ctx, m.cfg.WaitTimeout), done: make(chan result, 1)}
	select {
	case m.jobs <- j:
	default: // cannot happen while depth <= Max, but never block the caller
		m.depth.Add(-1)
		m.cfg.Obs.Inc(`queue_rejected_total{reason="full"}`, 1)
		return nil, ErrFull
	}
	m.cfg.Obs.Inc("queue_enqueued_total", 1)
	return wait(ctx, m.cfg.Obs, j.done, j.deadline)
}

func (m *Memory) worker() {
	defer m.wg.Done()
	for {
		select {
		case <-m.ctx.Done():
			return
		case j := <-m.jobs:
			resp, err := work(m.ctx, &m.cfg, j.model, j.req, j.deadline)
			m.depth.Add(-1)
			j.done <- result{resp, err}
		}
	}
}

func (m *Memory) Depth(context.Context) (int64, error) { return m.depth.Load(), nil }

func (m *Memory) Close() error {
	m.cancel()
	m.wg.Wait()
	return nil
}

// wait blocks the submitting request until its result, its deadline, or ctx.
func wait(ctx context.Context, obs Observer, done <-chan result, deadline time.Time) (*api.ChatResponse, error) {
	start := time.Now()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	defer func() {
		obs.Inc("queue_wait_seconds_sum", time.Since(start).Seconds())
		obs.Inc("queue_wait_seconds_count", 1)
	}()
	select {
	case r := <-done:
		if r.err == ErrTimeout {
			obs.Inc(`queue_rejected_total{reason="timeout"}`, 1)
		}
		return r.resp, r.err
	case <-timer.C:
		obs.Inc(`queue_rejected_total{reason="timeout"}`, 1)
		return nil, ErrTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
