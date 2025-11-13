package tokenbucket

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/h-hmz/rate-limiter/limiter"
)

func TestTokenBucket_InMemory(t *testing.T) {
	t.Run("Basic Refill and Burst Logic", func(t *testing.T) {
		// 1. Setup
		// Rate: 1 token/sec, Burst: 10 tokens
		clock := limiter.NewMockClock(time.Now())
		store := NewInMemoryStore()
		burst := int64(10)
		limiterInstance := New(1, burst, store, clock)

		ctx := context.Background()
		user := "Raditz"

		// 2. Consume Initial Burst (10 tokens)
		for range burst {
			isAllowed := limiterInstance.Allow(ctx, user)
			assert.True(t, isAllowed, "Request within burst should be allowed")
		}

		// 3. Verify Limit Reached
		isAllowed := limiterInstance.Allow(ctx, user)
		assert.False(t, isAllowed, "Request exceeing limit should be rejected (bucket empty)")

		// 4. Advance Clock & Verify Refill
		// Advance 1 second -> Should refill 1 token (Rate is 1/sec)
		clock.Advance(time.Second)

		isAllowed = limiterInstance.Allow(ctx, user)
		assert.True(t, isAllowed, "Request after 1s refill should be allowed")

		// 5. Verify Token Consumption
		// That single refilled token is now gone, next request must fail
		isAllowed = limiterInstance.Allow(ctx, user)
		assert.False(t, isAllowed, "Request should be rejected again (refilled token consumed)")
	})

	t.Run("Concurrency Safety", func(t *testing.T) {
		// Rate: 0 (no refill), Burst: 1.
		// This guarantees that exactly ONE request can ever succeed,
		// regardless of how many goroutines try simultaneously.
		wallClock := &limiter.WallClock{}
		store := NewInMemoryStore()
		limiterInstance := New(0, 1, store, wallClock)

		ctx := context.Background()
		user := "Nappa"

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

		assert.Equal(t, int64(1), successCount.Load(), "Should strictly enforce limit of 1 under high concurrency")
	})
}
