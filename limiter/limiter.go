package limiter

import (
	"context"
	"hash/fnv"
	"sync"
	"time"
)

const (
	// ContextKeyUserID is the key used to extract the user ID from the context.
	ContextKeyUserID = "UserID"

	// ShardCount determines the number of shards in the map.
	// It must be a power of 2 to allow bitwise optimization in getShard (x & (n-1)).
	// Higher values reduce lock contention but increase memory overhead.
	ShardCount = 256
)

// UserQuota holds the state for a single user's rate limit.
type UserQuota struct {
	tokens     float64   // Current number of tokens available. Float allows sub-token precision.
	lastRefill time.Time // Timestamp of the last token refill.
}

// shard represents a partition of the rate limiter storage.
// It has its own mutex to reduce global lock contention.
type shard struct {
	sync.Mutex
	users map[string]UserQuota
}

// RateLimiter implements a sharded token bucket rate limiter.
type RateLimiter struct {
	shards []*shard
	rate   float64 // Tokens added per second.
	burst  float64 // Maximum number of tokens allowed to accumulate.
}

// NewRateLimiter creates a new RateLimiter instance.
// rate: tokens per second.
// burst: maximum burst size.
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	rl := &RateLimiter{
		shards: make([]*shard, ShardCount),
		rate:   rate,
		burst:  float64(burst),
	}

	// Initialize each shard with its own map and lock.
	for i := range ShardCount {
		rl.shards[i] = &shard{
			users: make(map[string]UserQuota),
		}
	}

	return rl
}

// getShard calculates the shard index for a given key using FNV hashing.
// This ensures the same key always lands in the same shard.
func (r *RateLimiter) getShard(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	// Bitwise AND is equivalent to modulo for powers of 2, but faster.
	return r.shards[h.Sum32()&(ShardCount-1)]
}

// Allow checks if the request is allowed for the user found in the context.
// It returns true if allowed, false otherwise.
func (r *RateLimiter) Allow(ctx context.Context) bool {
	// Extract UserID from context.
	val := ctx.Value(ContextKeyUserID)
	userID, ok := val.(string)
	if !ok {
		// If UserID is missing or invalid, we fail-closed (deny access).
		// Alternatively, you could log an error or allow by default.
		return false
	}

	// Locate the specific shard for this user to avoid global locking.
	shard := r.getShard(userID)

	shard.Lock()
	defer shard.Unlock()

	now := time.Now()
	quota, exists := shard.users[userID]

	if !exists {
		// First time seeing this user: fill bucket to burst capacity.
		quota = UserQuota{
			tokens:     r.burst,
			lastRefill: now,
		}
	} else {
		// Calculate tokens to add based on time elapsed since last refill.
		delta := now.Sub(quota.lastRefill).Seconds()
		tokensToAdd := delta * r.rate

		quota.tokens += tokensToAdd

		// Clamp tokens to burst limit.
		if quota.tokens > r.burst {
			quota.tokens = r.burst
		}
		quota.lastRefill = now
	}

	// Check if user has enough tokens for 1 request.
	allowed := false
	if quota.tokens >= 1.0 {
		quota.tokens -= 1.0
		allowed = true
	}

	// Update the state in the map.
	shard.users[userID] = quota

	return allowed
}
