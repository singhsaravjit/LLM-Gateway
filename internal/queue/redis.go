package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/singhsaravjit/llm-gateway/internal/api"
	"github.com/singhsaravjit/llm-gateway/internal/provider"
)

const (
	group       = "workers"
	replyMaxLen = 1000
	replyTTL    = 10 * time.Minute
	blockFor    = time.Second // how long blocking reads wait before re-checking for shutdown
	opTimeout   = 200 * time.Millisecond
)

// RedisConfig adds the Redis-specific settings to Config.
type RedisConfig struct {
	Config
	NodeID string // unique per gateway process; replies for this node go to reply:<NodeID>
	Stream string // default "queue:provider"
	// Visibility: a job a worker has held this long without finishing is
	// assumed lost (worker crashed) and is reclaimed by another worker.
	// It must exceed WaitTimeout so a slow but live job is never run (and
	// billed) twice. Consequence: by the time a crashed worker's job is
	// reclaimed its client has timed out, so reclaiming cleans up the orphan
	// (accurate depth, no wasted provider call) rather than answering it.
	// Default WaitTimeout + 30s.
	Visibility time.Duration
}

// Redis is a queue shared by all gateway nodes:
//
//	Submit:  XADD queue:provider {id, node, deadline, model, req}, then wait
//	workers: XREADGROUP (group "workers") on every node -> pace -> provider
//	         -> XADD reply:<node> {id, resp | status+err} -> XACK + XDEL
//	reply reader: one goroutine per node reads reply:<node> and wakes the
//	         waiting request by id
//	reclaimer: XAUTOCLAIM jobs idle longer than Visibility (crashed worker)
//	         and remove them; they are past their deadline, so never sent
//
// Depth is XLEN: jobs are deleted when finished, so the stream holds exactly
// the waiting and in-progress jobs.
type Redis struct {
	cfg    RedisConfig
	c      redis.UniversalClient
	reply  string
	mu     sync.Mutex
	wait   map[string]chan result
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewRedis(ctx context.Context, c redis.UniversalClient, cfg RedisConfig) (*Redis, error) {
	cfg.defaults()
	if cfg.NodeID == "" {
		return nil, errors.New("queue: NodeID is required")
	}
	if cfg.Stream == "" {
		cfg.Stream = "queue:provider"
	}
	if cfg.Visibility <= 0 {
		cfg.Visibility = cfg.WaitTimeout + 30*time.Second
	}
	if cfg.Visibility <= cfg.WaitTimeout {
		return nil, fmt.Errorf("queue: visibility (%v) must exceed wait timeout (%v)", cfg.Visibility, cfg.WaitTimeout)
	}
	q := &Redis{cfg: cfg, c: c, reply: "reply:" + cfg.NodeID, wait: map[string]chan result{}}
	// Redis being down at startup is not fatal (like the other components):
	// workers create the group as soon as Redis answers.
	_ = q.ensureGroup(ctx)
	q.ctx, q.cancel = context.WithCancel(context.Background())
	q.wg.Add(1)
	go q.replyLoop()
	for i := 0; i < cfg.Workers; i++ {
		q.wg.Add(1)
		go q.workerLoop(fmt.Sprintf("%s-%d", cfg.NodeID, i))
	}
	if cfg.Workers > 0 {
		q.wg.Add(1)
		go q.reclaimLoop(cfg.NodeID + "-reclaim")
	}
	return q, nil
}

func (q *Redis) Submit(ctx context.Context, model string, req *api.ChatRequest) (*api.ChatResponse, error) {
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	// Bound check, then add. Concurrent submitters can overshoot Max slightly;
	// we never use XADD MAXLEN because trimming would drop accepted jobs.
	n, err := q.c.XLen(opCtx, q.cfg.Stream).Result()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if n >= q.cfg.Max {
		q.cfg.Obs.Inc(`queue_rejected_total{reason="full"}`, 1)
		return nil, ErrFull
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	id := newID()
	deadline := deadlineFor(ctx, q.cfg.WaitTimeout)
	done := make(chan result, 1)
	q.mu.Lock()
	q.wait[id] = done
	q.mu.Unlock()
	defer func() {
		q.mu.Lock()
		delete(q.wait, id)
		q.mu.Unlock()
	}()
	err = q.c.XAdd(opCtx, &redis.XAddArgs{Stream: q.cfg.Stream, Values: []any{
		"id", id, "node", q.cfg.NodeID, "deadline", deadline.UnixMilli(), "model", model, "req", body,
	}}).Err()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	q.cfg.Obs.Inc("queue_enqueued_total", 1)
	return wait(ctx, q.cfg.Obs, done, deadline)
}

func (q *Redis) Depth(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	return q.c.XLen(ctx, q.cfg.Stream).Result()
}

func (q *Redis) Close() error {
	q.cancel()
	q.wg.Wait()
	return nil
}

// ensureGroup creates the consumer group (and stream) if missing. "0": a group
// created after jobs were added still sees them.
func (q *Redis) ensureGroup(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	err := q.c.XGroupCreateMkStream(ctx, q.cfg.Stream, group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (q *Redis) workerLoop(consumer string) {
	defer q.wg.Done()
	for q.ctx.Err() == nil {
		res, err := q.c.XReadGroup(q.ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: consumer, Streams: []string{q.cfg.Stream, ">"}, Count: 1, Block: blockFor,
		}).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) && q.ctx.Err() == nil {
				q.cfg.Obs.Inc("queue_redis_errors_total", 1)
				if strings.HasPrefix(err.Error(), "NOGROUP") {
					_ = q.ensureGroup(q.ctx) // Redis was down at startup, or was flushed
				}
				q.sleep(100 * time.Millisecond)
			}
			continue
		}
		for _, s := range res {
			for _, msg := range s.Messages {
				q.handle(msg)
			}
		}
	}
}

func (q *Redis) reclaimLoop(consumer string) {
	defer q.wg.Done()
	every := min(q.cfg.Visibility/2, 5*time.Second)
	for q.sleep(every) {
		start := "0-0"
		for q.ctx.Err() == nil {
			msgs, next, err := q.c.XAutoClaim(q.ctx, &redis.XAutoClaimArgs{
				Stream: q.cfg.Stream, Group: group, Consumer: consumer,
				MinIdle: q.cfg.Visibility, Start: start, Count: 10,
			}).Result()
			if err != nil {
				break
			}
			for _, msg := range msgs {
				q.cfg.Obs.Inc("queue_reclaimed_total", 1)
				q.handle(msg)
			}
			if next == "0-0" || len(msgs) == 0 {
				break
			}
			start = next
		}
	}
}

// handle runs one job and sends the result to the node waiting for it.
func (q *Redis) handle(msg redis.XMessage) {
	id, _ := msg.Values["id"].(string)
	node, _ := msg.Values["node"].(string)
	model, _ := msg.Values["model"].(string)
	dl, _ := strconv.ParseInt(fmt.Sprint(msg.Values["deadline"]), 10, 64)
	var req api.ChatRequest
	var resp *api.ChatResponse
	var err error
	if jerr := json.Unmarshal([]byte(fmt.Sprint(msg.Values["req"])), &req); jerr != nil || id == "" || node == "" {
		err = fmt.Errorf("queue: malformed job %s", msg.ID)
	} else {
		resp, err = work(q.ctx, &q.cfg.Config, model, &req, time.UnixMilli(dl))
	}
	if q.ctx.Err() != nil {
		return // shutting down: leave the job pending so another node reclaims it
	}
	q.cfg.Obs.Inc(`queue_jobs_total{result="`+outcome(err)+`"}`, 1)
	if id != "" && node != "" && err != ErrTimeout { // expired: nobody is waiting
		q.sendReply(node, id, resp, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	q.c.XAck(ctx, q.cfg.Stream, group, msg.ID)
	q.c.XDel(ctx, q.cfg.Stream, msg.ID)
}

func outcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case err == ErrTimeout:
		return "expired"
	case IsRateLimited(err):
		return "upstream_429"
	}
	return "error"
}

func (q *Redis) sendReply(node, id string, resp *api.ChatResponse, err error) {
	vals := []any{"id", id}
	if err != nil {
		code := 0
		var se *provider.StatusError
		if errors.As(err, &se) {
			code = se.Code
		}
		vals = append(vals, "status", code, "err", err.Error())
	} else {
		b, _ := json.Marshal(resp)
		vals = append(vals, "resp", b)
	}
	stream := "reply:" + node
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	_, perr := q.c.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.XAdd(ctx, &redis.XAddArgs{Stream: stream, MaxLen: replyMaxLen, Approx: true, Values: vals})
		p.Expire(ctx, stream, replyTTL) // replies to a dead node disappear
		return nil
	})
	if perr != nil {
		q.cfg.Obs.Inc("queue_redis_errors_total", 1)
	}
}

// replyLoop delivers results addressed to this node to the waiting requests.
func (q *Redis) replyLoop() {
	defer q.wg.Done()
	last := "0-0" // read from the start so nothing sent before the first read is missed
	for q.ctx.Err() == nil {
		res, err := q.c.XRead(q.ctx, &redis.XReadArgs{Streams: []string{q.reply, last}, Count: 100, Block: blockFor}).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) && q.ctx.Err() == nil {
				q.cfg.Obs.Inc("queue_redis_errors_total", 1)
				q.sleep(100 * time.Millisecond)
			}
			continue
		}
		for _, s := range res {
			for _, msg := range s.Messages {
				last = msg.ID
				q.deliver(msg)
			}
		}
	}
}

func (q *Redis) deliver(msg redis.XMessage) {
	id, _ := msg.Values["id"].(string)
	q.mu.Lock()
	done, ok := q.wait[id]
	q.mu.Unlock()
	if !ok {
		return // the request already timed out or was cancelled
	}
	var r result
	if e, has := msg.Values["err"]; has {
		code, _ := strconv.Atoi(fmt.Sprint(msg.Values["status"]))
		if code > 0 {
			r.err = &provider.StatusError{Code: code, Body: fmt.Sprint(e)}
		} else {
			r.err = errors.New(fmt.Sprint(e))
		}
	} else {
		var resp api.ChatResponse
		if err := json.Unmarshal([]byte(fmt.Sprint(msg.Values["resp"])), &resp); err != nil {
			r.err = fmt.Errorf("queue: bad reply: %w", err)
		} else {
			r.resp = &resp
		}
	}
	select {
	case done <- r:
	default: // duplicate reply (job ran twice after a reclaim): first one wins
	}
}

// sleep waits d or until shutdown; it reports whether to keep running.
func (q *Redis) sleep(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-q.ctx.Done():
		return false
	}
}

func newID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
