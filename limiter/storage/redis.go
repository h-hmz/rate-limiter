package storage

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore[T any] struct {
	rdb *redis.Client
}

var _ Store[int] = (*RedisStore[int])(nil)

func NewRedisStore[T any](redisAddr string) *RedisStore[T] {
	return &RedisStore[T]{
		rdb: redis.NewClient(&redis.Options{
			Addr: redisAddr,
		}),
	}
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

		// Operation is commited only if the watched keys remain unchanged.
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

	// Retry if the key has been changed.
	maxRetries := 100
	for range maxRetries {
		err := r.rdb.Watch(ctx, txf, key)
		if err == nil {
			// Success.
			return finalState, finalAllowed, nil
		}
		if err == redis.TxFailedErr {
			if ctx.Err() != nil {
				return zero, false, ctx.Err()
			}
			continue // Optimistic lock lost. Retry.
		}
		// Return any other error.
		return zero, false, err
	}

	return zero, false, errors.New("redis AtomicUpdate: max retries exceeded")
}
