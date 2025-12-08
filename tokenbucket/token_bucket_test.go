package tokenbucket

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	limiter "github.com/h-hmz/rate-limiter"
	"github.com/h-hmz/rate-limiter/storage"
)

func TestTokenBucket_WithStores(t *testing.T) {
	t.Run("InMemory", func(t *testing.T) {
		inMemoryStoreFactory := func(clock limiter.Clock) storage.Store[State] { return storage.NewInMemoryStore[State](clock) }
		runTokenBucketTestSuite(t, inMemoryStoreFactory)
	})

	t.Run("Redis", func(t *testing.T) {
		redisStoreFactory := func(_ limiter.Clock) storage.Store[State] {
			redis := miniredis.RunT(t)
			return storage.NewRedisStore[State](redis.Addr())
		}
		runTokenBucketTestSuite(t, redisStoreFactory)
	})
}

func runTokenBucketTestSuite(t *testing.T, storeFactory func(clock limiter.Clock) storage.Store[State]) {
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
