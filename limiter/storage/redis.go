package storage

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type Initializable interface {
	IsInitialized() bool
}

type RedisStore[T Initializable] struct {
	rdb *redis.Client
}

// Verify at compile time that RedisStore implements *Store* interface
type interfaceValidator struct{}

func (c interfaceValidator) IsInitialized() bool { return true }

var _ Store[interfaceValidator] = (*RedisStore[interfaceValidator])(nil)

func NewRedisStore[T Initializable](redisAddr string) *RedisStore[T] {
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

		err := tx.HGetAll(ctx, key).Scan(&currentState)
		if err != nil {
			return err
		}

		// HGetAll returns empty struct if key is missing.
		if !currentState.IsInitialized() {
			currentState = init()
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
