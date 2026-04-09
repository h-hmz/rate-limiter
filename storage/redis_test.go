package storage

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type RStateMock struct {
	// HSET needs at least one field.
	// We add a dummy field so Redis has something to store.
	Val int64 `redis:"v"`
}

func (s RStateMock) IsInitialized() bool { return true }

func TestRedisStore_TTL(t *testing.T) {

	// Setup
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	store := NewRedisStore[RStateMock](redisClient)

	ttl := time.Minute

	ctx := context.Background()
	key := "android18"

	// Trigger key creation
	_, _, err := store.AtomicUpdate(ctx, key, ttl,
		func() RStateMock {
			return RStateMock{}
		},
		func(state RStateMock) (RStateMock, bool) {
			return state, true
		})
	require.NoError(t, err)

	exists, _ := redisClient.Exists(ctx, key).Result()
	assert.Equal(t, int64(1), exists, "Key should exist immediately")

	currentTTL, _ := redisClient.TTL(ctx, key).Result()
	assert.True(t, currentTTL > 0, "TTL should be set")

	mr.FastForward(59 * time.Second) //Fastforward time in Redis

	exists, _ = redisClient.Exists(ctx, key).Result()
	assert.Equal(t, int64(1), exists, "Key should exist after 59s")

	mr.FastForward(2 * time.Second)

	exists, _ = redisClient.Exists(ctx, key).Result()
	assert.Equal(t, int64(0), exists, "Key should be evicted after 61s")
}
