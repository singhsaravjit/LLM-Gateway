package budget

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// keyTTL keeps a month's counter around a little past the end of the month,
// then lets Redis delete it.
const keyTTL = 40 * 24 * time.Hour

// Redis tracks spend in budget:<team>:<yyyy-mm> (UTC month), an integer
// number of micro-dollars shared by every gateway node.
type Redis struct {
	client   redis.Cmdable
	limits   Limits
	failMode FailMode
	timeout  time.Duration
	now      func() time.Time
}

type RedisOption func(*Redis)

func WithFailMode(m FailMode) RedisOption     { return func(r *Redis) { r.failMode = m } }
func WithTimeout(d time.Duration) RedisOption { return func(r *Redis) { r.timeout = d } }

func NewRedis(client redis.Cmdable, l Limits, opts ...RedisOption) *Redis {
	r := &Redis{client: client, limits: l, failMode: FailOpen, timeout: 50 * time.Millisecond, now: time.Now}
	for _, o := range opts {
		o(r)
	}
	return r
}

func (r *Redis) key(team string) string { return "budget:" + team + ":" + monthKey(r.now()) }

func (r *Redis) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	if r.timeout > 0 {
		return context.WithTimeout(ctx, r.timeout)
	}
	return ctx, func() {}
}

func (r *Redis) Check(ctx context.Context, team string) (bool, error) {
	limit := r.limits.For(team)
	if limit <= 0 {
		return true, nil // unlimited: no Redis call at all
	}
	ctx, cancel := r.ctx(ctx)
	defer cancel()
	spent, err := r.client.Get(ctx, r.key(team)).Int64()
	if errors.Is(err, redis.Nil) {
		return true, nil
	}
	if err != nil {
		return r.failMode == FailOpen, fmt.Errorf("budget redis: %w", err)
	}
	return spent < toMicros(limit), nil
}

func (r *Redis) Add(ctx context.Context, team string, usd float64) (Spend, error) {
	limit := r.limits.For(team)
	micros := toMicros(usd)
	if micros <= 0 {
		return Spend{LimitUSD: limit}, nil
	}
	ctx, cancel := r.ctx(ctx)
	defer cancel()
	k := r.key(team)
	var incr *redis.IntCmd
	_, err := r.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		incr = p.IncrBy(ctx, k, micros)
		p.Expire(ctx, k, keyTTL)
		return nil
	})
	if err != nil {
		return Spend{LimitUSD: limit}, fmt.Errorf("budget redis: %w", err)
	}
	total := incr.Val()
	return Spend{
		TotalUSD:     fromMicros(total),
		LimitUSD:     limit,
		CrossedAlert: crossed(total-micros, total, limit, r.limits.AlertFraction),
	}, nil
}
