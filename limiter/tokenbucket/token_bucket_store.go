package tokenbucket

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/h-hmz/rate-limiter/limiter"
	"github.com/h-hmz/rate-limiter/limiter/internal/shardedmap"
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

// entry wraps the domain state with storage metadata (TTL)
type entry struct {
	state     State
	expiresAt time.Time
}

type InMemoryStore struct {
	data  shardedmap.ShardedMap[entry]
	clock limiter.Clock
}

var _ Store = (*InMemoryStore)(nil)

func NewInMemoryStore(clock limiter.Clock) *InMemoryStore {
	return &InMemoryStore{
		data:  shardedmap.NewShardedMap[entry](256),
		clock: clock,
	}

	//start background process for cleaning ttls
}

func (r *InMemoryStore) AtomicUpdate(ctx context.Context, key string, ttl time.Duration, init func() State, fn func(State) (State, bool)) (State, bool, error) {

	now := r.clock.Now()

	wrapperInit := func() entry {
		return entry{
			state:     init(),
			expiresAt: now.Add(ttl),
		}
	}
	wrapperFn := func(current entry) (entry, bool) {

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

func (r *InMemoryStore) DeleteExpiredKeys() {

	now := r.clock.Now()
	r.data.RemoveIf(func(val entry) bool {
		// Checks if the entry has expired relative to the current clock time
		return now.After(val.expiresAt)
	})

}

func (r *InMemoryStore) StartGC(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			r.DeleteExpiredKeys()
		}
	}()
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
