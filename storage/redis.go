package storage

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore[T any] struct {
	rdb *redis.Client
}

var _ Store[int] = (*RedisStore[int])(nil)

func NewRedisStore[T any](client *redis.Client) *RedisStore[T] {
	return &RedisStore[T]{rdb: client}
}

func (r RedisStore[T]) AtomicUpdate(ctx context.Context, key string, ttl time.Duration, init func() T, fn func(T) (T, bool)) (T, bool, error) {

	var finalAllowed bool
	var finalState T
	var zero T

	// Transactional function, see: https://redis.uptrace.dev/guide/go-redis-pipelines.html#transactions
	txf := func(tx *redis.Tx) error {

		var currentState T

		cmd := tx.HGetAll(ctx, key)
		vals, err := cmd.Result()
		if err != nil {
			return err
		}
		if len(vals) == 0 {
			currentState = init()
		} else {
			if err := cmd.Scan(&currentState); err != nil {
				return err
			}
		}

		// Actual operation (local in optimistic lock)
		newState, allowed := fn(currentState)

		// Operation is committed only if the watched keys remain unchanged.
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, key, newState)
			if ttl > 0 {
				pipe.Expire(ctx, key, ttl)
			}
			return nil
		})

		if err == nil {
			finalState = newState
			finalAllowed = allowed
		}

		return err
	}

	// Retry on optimistic lock failure with exponential backoff + full jitter
	// to avoid thundering herd under contention.
	// See: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	const (
		maxRetries = 20
		baseDelay  = time.Millisecond
		maxDelay   = 16 * time.Millisecond
	)

	for attempt := range maxRetries {
		err := r.rdb.Watch(ctx, txf, key)
		if err == nil {
			// Success.
			return finalState, finalAllowed, nil
		}
		if err != redis.TxFailedErr {
			// Return any other error.
			return zero, false, err
		}
		if ctx.Err() != nil {
			return zero, false, ctx.Err()
		}

		// Optimistic lock lost. Backoff and retry.
		backoffCap := min(maxDelay, baseDelay*time.Duration(1<<attempt))
		sleep := time.Duration(rand.Int64N(int64(backoffCap)))
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return zero, false, ctx.Err()
		}
	}

	return zero, false, errors.New("redis AtomicUpdate: max retries exceeded")
}
