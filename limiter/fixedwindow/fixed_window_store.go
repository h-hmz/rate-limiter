package fixedwindow

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Store interface {
	Get(ctx context.Context, key string) (State, error)
	Set(ctx context.Context, key string, val State) error
}

type InMemoryStore struct {
	usersMap map[string]State
}

func NewInMemory() InMemoryStore {
	return InMemoryStore{
		usersMap: make(map[string]State),
	}
}

func (r *InMemoryStore) Get(ctx context.Context, key string) (State, error) {
	val, ok := r.usersMap[key]

	if !ok {
		return State{}, ErrNotFound
	}
	return val, nil
}

func (r *InMemoryStore) Set(ctx context.Context, key string, val State) error {
	r.usersMap[key] = val
	return nil
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
