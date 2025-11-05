package limiter

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

/*
NOTE ON pressure points:
- UserQuota lives in Storage.go, even though conceptually it belongs to the `Token Bucket` algorithm.
This is a pressure point, a reasonable flaw awaiting refactoring later.

- UserQuota leaking Redis tags is a smell. It tells that storage concerns are trying to influence domain state representation.const
Eventually, we would want:
	- a domain struct
	- a storage representation
	- a mapping between them


At this stage, try to not make the abstraction more general.
The next good abstraction will not come from thinking harder, but from one of these pressures:
	- Adding atomicity (Lua, transactions, CAS)
	- Adding eviction/TTL
	- Adding observability (metrics, traces)

When that pressure arrives, this abstraction will either bend gracefully or crack loudly (either outcome is a success).
*/

// go-redis uses Reflection to look inside the struct
type UserQuota struct {
	Tokens     int64     `redis:"tokens"`
	LastRefill time.Time `redis:"last_refill"`
}

type Storage interface {
	Get(ctx context.Context, key string) (UserQuota, error)
	Set(ctx context.Context, key string, quota UserQuota) error
}

type InMemoryStore struct {
	usersMap map[string]UserQuota
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		usersMap: make(map[string]UserQuota),
	}
}

func (r *InMemoryStore) Get(ctx context.Context, key string) (UserQuota, error) {
	data, exists := r.usersMap[key]
	if !exists {
		return UserQuota{}, ErrNotFound
	}

	return data, nil
}

func (r *InMemoryStore) Set(ctx context.Context, key string, val UserQuota) error {
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

func (r *RedisStore) Get(ctx context.Context, key string) (UserQuota, error) {
	var quota UserQuota

	//Scan() automatically maps from redis hash to struct tags
	err := r.rdb.HGetAll(ctx, key).Scan(&quota)
	if err != nil {
		return UserQuota{}, err
	}

	if quota.LastRefill.IsZero() {
		return UserQuota{}, ErrNotFound
	}

	return quota, nil
}

func (r *RedisStore) Set(ctx context.Context, key string, val UserQuota) error {
	return r.rdb.HSet(ctx, key, val).Err()
}
