package tokenbucket

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/h-hmz/rate-limiter/limiter"
)

func TestTokenBucket_WithStores(t *testing.T) {
	t.Run("InMemory", func(t *testing.T) {
		inMemoryStoreFactory := func(clock limiter.Clock) Store { return NewInMemoryStore(clock) }
		runTokenBucketTestSuite(t, inMemoryStoreFactory)
	})

	t.Run("Redis", func(t *testing.T) {
		redisStoreFactory := func(_ limiter.Clock) Store {
			redis := miniredis.RunT(t)
			return NewRedisStore(redis.Addr())
		}
		runTokenBucketTestSuite(t, redisStoreFactory)
	})
}

func runTokenBucketTestSuite(t *testing.T, storeFactory func(clock limiter.Clock) Store) {
	t.Helper()

	t.Run("Basic Refill and Burst Logic", func(t *testing.T) {
		// 1. Setup
		// Rate: 1 token/sec, Burst: 10 tokens
		start := time.Unix(int64(time.Minute), 0) // Jan 1, 1970, 00:01:00 UTC
		clock := limiter.NewMockClock(start)
		burst := int64(10)
		rate := float64(1)
		limiterInstance := New(rate, burst, storeFactory(clock), clock)

		ctx := context.Background()
		key := "raditz"

		// 2. Consume Initial Burst (10 tokens)
		for range burst {
			isAllowed, err := limiterInstance.Allow(ctx, key)
			require.NoError(t, err)
			assert.True(t, isAllowed, "Request within burst should be allowed")
		}

		// 3. Verify Limit Reached
		isAllowed, _ := limiterInstance.Allow(ctx, key)
		assert.False(t, isAllowed, "Request exceeing limit should be rejected (bucket empty)")

		// 4. Advance Clock & Verify Refill
		// Advance 1 second -> Should refill 1 token (Rate is 1/sec)
		clock.Advance(time.Second)

		isAllowed, _ = limiterInstance.Allow(ctx, key)
		assert.True(t, isAllowed, "Request after 1s refill should be allowed")

		// 5. Verify Token Consumption
		// That single refilled token is now gone, next request must fail
		isAllowed, _ = limiterInstance.Allow(ctx, key)
		assert.False(t, isAllowed, "Request should be rejected again (refilled token consumed)")
	})

	t.Run("Concurrency Safety", func(t *testing.T) {
		// Rate: 0 (no refill), Burst: 1.
		// This guarantees that exactly ONE request can ever succeed,
		// regardless of how many goroutines try simultaneously.
		wallClock := &limiter.WallClock{}
		limiterInstance := New(0, 1, storeFactory(wallClock), wallClock)

		ctx := context.Background()
		key := "nappa"

		var successCount atomic.Int64
		var wg sync.WaitGroup
		totalRequests := 100

		// Execute Concurrent Requests
		for range totalRequests {
			wg.Add(1)
			go func() {
				defer wg.Done()
				isAllowed, _ := limiterInstance.Allow(ctx, key)
				if isAllowed {
					successCount.Add(1)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int64(1), successCount.Load(), "Should strictly enforce limit of 1 under high concurrency")
	})
}

func TestTokenBucketTTL_Redis(t *testing.T) {

	// Setup
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	store := NewRedisStore(mr.Addr())

	start := time.Now()
	clock := limiter.NewMockClock(start)
	l := New(1.0, 60, store, clock) // ttl = burst/rate = 60s

	ctx := context.Background()
	key := "android18"

	// Trigger key creation
	_, err := l.Allow(ctx, key)
	require.NoError(t, err)

	exists, _ := redisClient.Exists(ctx, key).Result()
	assert.Equal(t, int64(1), exists, "Key should exist immediately")

	ttl, _ := redisClient.TTL(ctx, key).Result()
	assert.True(t, ttl > 0, "TTL should be set")

	mr.FastForward(59 * time.Second) //Fastforward time in Redis

	exists, _ = redisClient.Exists(ctx, key).Result()
	assert.Equal(t, int64(1), exists, "Key should exist after 59s")

	mr.FastForward(2 * time.Second)

	exists, _ = redisClient.Exists(ctx, key).Result()
	assert.Equal(t, int64(0), exists, "Key should be evicted after 61s")
}

func TestTokenBucketTTL_InMemoryGC(t *testing.T) {

	// Setup
	start := time.Now()
	clock := limiter.NewMockClock(start)
	store := NewInMemoryStore(clock)

	l := New(1.0, 60, store, clock) // ttl = burst/rate = 60s

	ctx := context.Background()
	key := "android17"

	// Trigger key creation
	_, err := l.Allow(ctx, key)
	require.NoError(t, err)

	entry, exists := store.data.Get(key)
	assert.True(t, exists, "Key should exist immediately")
	assert.True(t, entry.expiresAt.After(clock.Now()), "expiresAt should be set")

	clock.Advance(59 * time.Second) //Fastforward time in Mock Clock

	store.DeleteExpiredKeys()
	entry, exists = store.data.Get(key)
	assert.True(t, exists, "Key should exist after 59s")

	clock.Advance(2 * time.Second) //Fastforward time in Mock Clock

	store.DeleteExpiredKeys()
	entry, exists = store.data.Get(key)
	assert.False(t, exists, "Key should be evicted by GC after 61s")
}
