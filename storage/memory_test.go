package storage

import (
	"context"
	"testing"
	"time"

	limiter "github.com/h-hmz/rate-limiter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type StateMock struct{}

func (s StateMock) IsInitialized() bool { return true }

func TestInMemoryStore_TTL_GC(t *testing.T) {

	// Setup
	start := time.Now()
	clock := limiter.NewMockClock(start)
	store := NewInMemoryStore[StateMock](clock)

	ctx := context.Background()
	key := "android17"
	ttl := time.Minute

	// Trigger key creation
	_, _, err := store.AtomicUpdate(ctx, key, ttl,
		func() StateMock {
			return StateMock{}
		},
		func(state StateMock) (StateMock, bool) {
			return state, true
		})

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
