package fixedwindow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/h-hmz/rate-limiter/limiter"
)

func TestFixedWindow_InMemory(t *testing.T) {
	t.Run("Basic Limits and Window Reset", func(t *testing.T) {
		// 1. Setup
		clock := limiter.NewMockClock(time.Now())
		store := NewInMemoryStore()
		limit := int64(3)
		windowDuration := time.Minute

		limiterInstance := New(limit, windowDuration, &store, clock)

		ctx := context.Background()
		user := "Recoome"

		// 2. Consume all tokens
		for range limit {
			allowed := limiterInstance.Allow(ctx, user)
			assert.True(t, allowed, "Request within limit should be allowed")
		}

		// 3. Exceed Limit
		allowed := limiterInstance.Allow(ctx, user)
		assert.False(t, allowed, "Request exceeding limit should be rejected")

		// 4. Advance Clock (Halfway) -> Should still reject
		clock.Advance(30 * time.Second)
		allowed = limiterInstance.Allow(ctx, user)
		assert.False(t, allowed, "Request in same window should be rejected")

		// 5. Advance Clock (New Window) -> Should allow
		clock.Advance(31 * time.Second) // Total > 60s
		allowed = limiterInstance.Allow(ctx, user)
		assert.True(t, allowed, "Request in new window should be allowed")
	})

	t.Run("Concurrency Safety", func(t *testing.T) {
		store := NewInMemoryStore()
		clock := &limiter.WallClock{} // Use real clock for goroutine scheduling

		limit := int64(10)
		limiterInstance := New(limit, time.Minute, &store, clock)

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
				if limiterInstance.Allow(ctx, user) {
					successCount.Add(1)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, limit, successCount.Load(), "Should strictly enforce limit under concurrency")
	})
}
