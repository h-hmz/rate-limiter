package storage

import (
	"context"
	"time"

	"github.com/h-hmz/rate-limiter/limiter"
	"github.com/h-hmz/rate-limiter/limiter/internal/shardedmap"
)

type InMemoryStore[T any] struct {
	data  shardedmap.ShardedMap[entry[T]]
	clock limiter.Clock
}

var _ Store[int] = (*InMemoryStore[int])(nil)

func NewInMemoryStore[T any](clock limiter.Clock) *InMemoryStore[T] {
	return &InMemoryStore[T]{
		data:  shardedmap.NewShardedMap[entry[T]](256),
		clock: clock,
	}
}

func (r *InMemoryStore[T]) AtomicUpdate(ctx context.Context, key string, ttl time.Duration, init func() T, fn func(T) (T, bool)) (T, bool, error) {

	now := r.clock.Now()

	wrapperInit := func() entry[T] {
		return entry[T]{
			state:     init(),
			expiresAt: now.Add(ttl),
		}
	}
	wrapperFn := func(current entry[T]) (entry[T], bool) {

		// Lazy expiration
		if ttl > 0 && current.expiresAt.Before(now) {
			current.state = init()
		}

		newState, isAllowed := fn(current.state)

		current.state = newState
		if ttl > 0 {
			current.expiresAt = now.Add(ttl)
		}

		return current, isAllowed
	}

	val, ok := r.data.WithShard(key, wrapperInit, wrapperFn)
	return val.state, ok, nil
}

func (r *InMemoryStore[T]) DeleteExpiredKeys() {

	now := r.clock.Now()
	r.data.RemoveIf(func(val entry[T]) bool {
		// Checks if the entry has expired relative to the current clock time
		return now.After(val.expiresAt)
	})

}

func (r *InMemoryStore[T]) StartGC(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			r.DeleteExpiredKeys()
		}
	}()
}
