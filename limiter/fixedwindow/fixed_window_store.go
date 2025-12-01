package fixedwindow

import (
	"context"
	"errors"
	"time"

	"github.com/h-hmz/rate-limiter/limiter/internal/shardedmap"
	"github.com/redis/go-redis/v9"
)

type Store interface {
	AtomicUpdate(
		ctx context.Context,
		key string,
		ttl time.Duration,
		init func() State,
		fn func(State) (State, bool),
	) (State, bool, error)
}

type InMemoryStore struct {
	data shardedmap.ShardedMap[State]
}

var _ Store = (*InMemoryStore)(nil)

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		data: shardedmap.NewShardedMap[State](256),
	}
}

func (r *InMemoryStore) AtomicUpdate(ctx context.Context, key string, ttl time.Duration, init func() State, fn func(State) (State, bool)) (State, bool, error) {
	val, ok := r.data.WithShard(key, init, fn)
	return val, ok, nil
}

type RedisStore struct {
	rdb *redis.Client
}

var _ Store = (*RedisStore)(nil)

func NewRedisStore(redisAddr string) *RedisStore {
	return &RedisStore{
		rdb: redis.NewClient(&redis.Options{
			Addr: redisAddr,
		}),
	}
}

func (r *RedisStore) AtomicUpdate(ctx context.Context, key string, ttl time.Duration, init func() State, fn func(State) (State, bool)) (State, bool, error) {

	var finalAllowed bool
	var finalState State

	// Transactional function, see: https://redis.uptrace.dev/guide/go-redis-pipelines.html#transactions
	txf := func(tx *redis.Tx) error {

		var currentState State

		err := tx.HGetAll(ctx, key).Scan(&currentState)
		if err != nil {
			return err
		}

		// HGetAll returns empty struct if key is missing.
		if !currentState.isInitialized() {
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
				return State{}, false, ctx.Err()
			}
			continue // Optimistic lock lost. Retry.
		}
		// Return any other error.
		return State{}, false, err
	}

	return State{}, false, errors.New("redis AtomicUpdate: max retries exceeded")
}
