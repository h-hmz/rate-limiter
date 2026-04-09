package fixedwindow

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

	limiter "github.com/h-hmz/rate-limiter"
	"github.com/h-hmz/rate-limiter/storage"
)

func TestFixedWindow_WithStores(t *testing.T) {
	t.Run("InMemory", func(t *testing.T) {
		inMemoryStoreFactory := func(clock limiter.Clock) storage.Store[State] { return storage.NewInMemoryStore[State](clock) }
		runFixedWindowTestSuite(t, inMemoryStoreFactory)
	})

	t.Run("Redis", func(t *testing.T) {
		redisStoreFactory := func(_ limiter.Clock) storage.Store[State] {
			mr := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			return storage.NewRedisStore[State](client)
		}
		runFixedWindowTestSuite(t, redisStoreFactory)
	})
}
func runFixedWindowTestSuite(t *testing.T, storeFactory func(clock limiter.Clock) storage.Store[State]) {
	t.Helper()

	t.Run("Basic Limits and Window Reset", func(t *testing.T) {
		// 1. Setup
		start := time.Unix(int64(time.Minute), 0) // Jan 1, 1970, 00:01:00 UTC
		clock := limiter.NewMockClock(start)
		limit := int64(3)
		windowDuration := time.Minute

		limiterInstance := New(limit, windowDuration, storeFactory(clock), clock)

		ctx := context.Background()
		key := "recoome"

		// 2. Consume all tokens
		for range limit {
			result, err := limiterInstance.Allow(ctx, key)
			require.NoError(t, err)
			assert.True(t, result.Allowed, "Request within limit should be allowed")
		}

		// 3. Exceed Limit
		result, err := limiterInstance.Allow(ctx, key)
		require.NoError(t, err)
		assert.False(t, result.Allowed, "Request exceeding limit should be rejected")

		// 4. Advance Clock (Halfway) -> Should still reject
		clock.Advance(30 * time.Second)
		result, err = limiterInstance.Allow(ctx, key)
		require.NoError(t, err)
		assert.False(t, result.Allowed, "Request in same window should be rejected")

		// 5. Advance Clock (New Window) -> Should allow
		clock.Advance(31 * time.Second) // Total > 60s
		result, err = limiterInstance.Allow(ctx, key)
		require.NoError(t, err)
		assert.True(t, result.Allowed, "Request in new window should be allowed")
	})

	t.Run("Concurrency Safety", func(t *testing.T) {
		clock := &limiter.WallClock{} // Use real clock for goroutine scheduling

		limit := int64(10)
		limiterInstance := New(limit, time.Minute, storeFactory(clock), clock)

		ctx := context.Background()
		key := "jeice"

		var successCount atomic.Int64
		var wg sync.WaitGroup
		totalRequests := 100

		// Execute Concurrent Requests
		for range totalRequests {
			wg.Add(1)
			go func() {
				defer wg.Done()
				result, err := limiterInstance.Allow(ctx, key)
				require.NoError(t, err)
				if result.Allowed {
					successCount.Add(1)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, limit, successCount.Load(), "Should strictly enforce limit under concurrency")
	})
}
