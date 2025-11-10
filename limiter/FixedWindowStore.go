package limiter

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type FixedWindowStore interface {
	Get(ctx context.Context, key string) (FixedWindowState, error)
	Set(ctx context.Context, key string, val FixedWindowState) error
}

type FixedWindowInMemoryStore struct {
	usersMap map[string]FixedWindowState
}

func NewFixedWindowInMemory() FixedWindowInMemoryStore {
	return FixedWindowInMemoryStore{
		usersMap: make(map[string]FixedWindowState),
	}
}

func (r *FixedWindowInMemoryStore) Get(ctx context.Context, key string) (FixedWindowState, error) {
	val, ok := r.usersMap[key]

	if !ok {
		return FixedWindowState{}, ErrNotFound
	}
	return val, nil
}

func (r *FixedWindowInMemoryStore) Set(ctx context.Context, key string, val FixedWindowState) error {
	r.usersMap[key] = val
	return nil
}

type FixedWindowRedisStore struct {
	rdb *redis.Client
}

func NewFixedWindowRedisStore(addr string, port string) *FixedWindowRedisStore {
	return &FixedWindowRedisStore{
		rdb: redis.NewClient(&redis.Options{
			Addr: addr + ":" + port,
		}),
	}
}

func (r *FixedWindowRedisStore) Get(ctx context.Context, key string) (FixedWindowState, error) {
	var val FixedWindowState

	//Scan() automatically maps from redis hash to struct tags
	err := r.rdb.HGetAll(ctx, key).Scan(&val)
	if err != nil {
		return FixedWindowState{}, err
	}

	if val.lastWindowID == 0 {
		return FixedWindowState{}, ErrNotFound
	}

	return val, nil
}

func (r *FixedWindowRedisStore) Set(ctx context.Context, key string, val FixedWindowState) error {
	return r.rdb.HSet(ctx, key, val).Err()
}
