package fixedwindow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/h-hmz/rate-limiter/limiter"
)

func TestFixedWindow_WithStores(t *testing.T) {
	t.Run("InMemory", func(t *testing.T) {
		inMemoryStoreFactory := func() Store { return NewInMemoryStore() }
		runFixedWindowTestSuite(t, inMemoryStoreFactory)
	})

	t.Run("Redis", func(t *testing.T) {
		redisStoreFactory := func() Store {
			redis := miniredis.RunT(t)
			return NewRedisStore(redis.Addr())
		}
		runFixedWindowTestSuite(t, redisStoreFactory)
	})
}
func runFixedWindowTestSuite(t *testing.T, storeFactory func() Store) {
	t.Helper()

	t.Run("Basic Limits and Window Reset", func(t *testing.T) {
		// 1. Setup
		start := time.Unix(int64(time.Minute), 0) // Jan 1, 1970, 00:01:00 UTC
		clock := limiter.NewMockClock(start)
		limit := int64(3)
		windowDuration := time.Minute

		limiterInstance := New(limit, windowDuration, storeFactory(), clock)

		ctx := context.Background()
		user := "Recoome"

		// 2. Consume all tokens
		for range limit {
			allowed, err := limiterInstance.Allow(ctx, user)
			require.NoError(t, err)
			assert.True(t, allowed, "Request within limit should be allowed")
		}

		// 3. Exceed Limit
		allowed, err := limiterInstance.Allow(ctx, user)
		require.NoError(t, err)
		assert.False(t, allowed, "Request exceeding limit should be rejected")

		// 4. Advance Clock (Halfway) -> Should still reject
		clock.Advance(30 * time.Second)
		allowed, err = limiterInstance.Allow(ctx, user)
		require.NoError(t, err)
		assert.False(t, allowed, "Request in same window should be rejected")

		// 5. Advance Clock (New Window) -> Should allow
		clock.Advance(31 * time.Second) // Total > 60s
		allowed, err = limiterInstance.Allow(ctx, user)
		require.NoError(t, err)
		assert.True(t, allowed, "Request in new window should be allowed")
	})

	t.Run("Concurrency Safety", func(t *testing.T) {
		clock := &limiter.WallClock{} // Use real clock for goroutine scheduling

		limit := int64(10)
		limiterInstance := New(limit, time.Minute, storeFactory(), clock)

		ctx := context.Background()
		user := "Jeice"

		var successCount atomic.Int64
		var wg sync.WaitGroup
		totalRequests := 100

		// Execute Concurrent Requests
		for range totalRequests {
			wg.Add(1)
			go func() {
				defer wg.Done()
				isAllowed, err := limiterInstance.Allow(ctx, user)
				require.NoError(t, err)
				if isAllowed {
					successCount.Add(1)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, limit, successCount.Load(), "Should strictly enforce limit under concurrency")
	})

}
