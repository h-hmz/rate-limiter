package tokenbucket

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/h-hmz/rate-limiter/limiter"
)

func TestTokenBucket(t *testing.T) {
	t.Run("Token Bucket algorithm", func(t *testing.T) {
		mockClock := limiter.NewMockClock(time.Now())
		tokenBucket := New(1, 10,
			NewInMemoryStore(),
			mockClock)

		ctx := context.Background()
		var isAllowed bool

		user1 := "Raditz"
		for range 10 { // spend all tokens
			isAllowed = tokenBucket.Allow(ctx, user1)
			assert.True(t, isAllowed)
		}

		isAllowed = tokenBucket.Allow(ctx, user1)
		assert.False(t, isAllowed)

		mockClock.Advance(time.Second)

		isAllowed = tokenBucket.Allow(ctx, user1)
		assert.True(t, isAllowed)
		isAllowed = tokenBucket.Allow(ctx, user1)
		assert.False(t, isAllowed)

	})
}
