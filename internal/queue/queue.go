// Package queue — owner: Saravjit.
//
// Overflow queue for requests that would exceed the provider's rate limit.
// Workers drain it at a safe rate; when full, the gateway rejects with 503
// (backpressure). Production target: SQS or Redis Streams (decide in week 1).
// TODO(saravjit): implementation, worker pool, queue depth/wait metrics (Exp. 5).
package queue

import (
	"context"
	"errors"
)

var ErrFull = errors.New("queue full")

// Job is a unit of work waiting for provider capacity.
type Job interface {
	Run(ctx context.Context)
}

type Queue interface {
	Enqueue(ctx context.Context, j Job) error // returns ErrFull on backpressure
	Depth() int
}
