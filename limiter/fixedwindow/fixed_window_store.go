package fixedwindow

import (
	"context"

	"github.com/h-hmz/rate-limiter/limiter/internal/shardedmap"
	"github.com/redis/go-redis/v9"
)

type Store interface {
	AtomicUpdate(
		ctx context.Context,
		key string,
		init func() State,
		fn func(State) (State, bool),
	) (State, bool)
}

type InMemoryStore struct {
	data shardedmap.ShardedMap[State]
}

var _ Store = (*InMemoryStore)(nil)

func NewInMemoryStore() InMemoryStore {
	return InMemoryStore{
		data: shardedmap.NewShardedMap[State](256),
	}
}

func (r *InMemoryStore) AtomicUpdate(ctx context.Context, key string, init func() State, fn func(State) (State, bool)) (State, bool) {
	val, ok := r.data.WithShard(key, init, fn)
	return val, ok
}

type RedisStore struct {
	rdb *redis.Client
}

func NewRedisStore(addr string, port string) *RedisStore {
	return &RedisStore{
		rdb: redis.NewClient(&redis.Options{
			Addr: addr + ":" + port,
		}),
	}
}

func (r *RedisStore) Get(ctx context.Context, key string) (State, error) {
	var val State

	//Scan() automatically maps from redis hash to struct tags
	err := r.rdb.HGetAll(ctx, key).Scan(&val)
	if err != nil {
		return State{}, err
	}

	if val.lastWindowID == 0 {
		return State{}, ErrNotFound
	}

	return val, nil
}

func (r *RedisStore) Set(ctx context.Context, key string, val State) error {
	return r.rdb.HSet(ctx, key, val).Err()
}
