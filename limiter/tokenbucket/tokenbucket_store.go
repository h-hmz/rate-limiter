package tokenbucket

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Store interface {
	Get(ctx context.Context, key string) (State, error)
	Set(ctx context.Context, key string, quota State) error
}

type InMemoryStore struct {
	usersMap map[string]State
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		usersMap: make(map[string]State),
	}
}

func (r *InMemoryStore) Get(ctx context.Context, key string) (State, error) {
	data, exists := r.usersMap[key]
	if !exists {
		return State{}, ErrNotFound
	}

	return data, nil
}

func (r *InMemoryStore) Set(ctx context.Context, key string, val State) error {
	r.usersMap[key] = val
	return nil
}

type RedisStore struct {
	rdb *redis.Client
}

func NewTokenBucketRedisStore(addr string, port string) *RedisStore {
	return &RedisStore{
		rdb: redis.NewClient(&redis.Options{
			Addr: addr + ":" + port,
		}),
	}
}

func (r *RedisStore) Get(ctx context.Context, key string) (State, error) {
	var quota State

	//Scan() automatically maps from redis hash to struct tags
	err := r.rdb.HGetAll(ctx, key).Scan(&quota)
	if err != nil {
		return State{}, err
	}

	if quota.LastRefill.IsZero() {
		return State{}, ErrNotFound
	}

	return quota, nil
}

func (r *RedisStore) Set(ctx context.Context, key string, val State) error {
	return r.rdb.HSet(ctx, key, val).Err()
}
