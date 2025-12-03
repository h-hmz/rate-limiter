package storage

import (
	"context"
	"time"
)

type Store[T any] interface {
	AtomicUpdate(
		ctx context.Context,
		key string,
		ttl time.Duration,
		init func() T,
		fn func(T) (T, bool),
	) (T, bool, error)
}

// entry wraps the domain state with storage metadata (TTL)
type entry[T any] struct {
	state     T
	expiresAt time.Time
}

func (e entry[T]) GetExpiresAt() time.Time {
	return e.expiresAt
}
