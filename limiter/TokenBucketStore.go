package limiter

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type TokenBucketStore interface {
	Get(ctx context.Context, key string) (TokenBucketState, error)
	Set(ctx context.Context, key string, quota TokenBucketState) error
}

type TokenBucketInMemoryStore struct {
	usersMap map[string]TokenBucketState
}

func NewTokenBucketInMemoryStore() *TokenBucketInMemoryStore {
	return &TokenBucketInMemoryStore{
		usersMap: make(map[string]TokenBucketState),
	}
}

func (r *TokenBucketInMemoryStore) Get(ctx context.Context, key string) (TokenBucketState, error) {
	data, exists := r.usersMap[key]
	if !exists {
		return TokenBucketState{}, ErrNotFound
	}

	return data, nil
}

func (r *TokenBucketInMemoryStore) Set(ctx context.Context, key string, val TokenBucketState) error {
	r.usersMap[key] = val
	return nil
}

type TokenBucketRedisStore struct {
	rdb *redis.Client
}

func NewTokenBucketRedisStore(addr string, port string) *TokenBucketRedisStore {
	return &TokenBucketRedisStore{
		rdb: redis.NewClient(&redis.Options{
			Addr: addr + ":" + port,
		}),
	}
}

func (r *TokenBucketRedisStore) Get(ctx context.Context, key string) (TokenBucketState, error) {
	var quota TokenBucketState

	//Scan() automatically maps from redis hash to struct tags
	err := r.rdb.HGetAll(ctx, key).Scan(&quota)
	if err != nil {
		return TokenBucketState{}, err
	}

	if quota.LastRefill.IsZero() {
		return TokenBucketState{}, ErrNotFound
	}

	return quota, nil
}

func (r *TokenBucketRedisStore) Set(ctx context.Context, key string, val TokenBucketState) error {
	return r.rdb.HSet(ctx, key, val).Err()
}
