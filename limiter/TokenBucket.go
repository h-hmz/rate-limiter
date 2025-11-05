package limiter

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("key not found in storage")

type TokenBucket struct {
	store Storage
	clock Clock
	rate  float64 //tokens refill rate per second
	burst int64
}

func NewTokenBucket(rate float64, burst int64, store Storage, clock Clock) *TokenBucket {
	return &TokenBucket{
		store: store,
		clock: clock,
		rate:  rate,
		burst: burst,
	}
}

// Time is a dependency, needs to be injected and not bake time.Now() into algo logic
// DO NOT ask "how can I mock time". Ask "Who is allowed to know what time it is?".
func (r *TokenBucket) Allow(ctx context.Context, key string) bool {
	userQuota, err := r.store.Get(ctx, key)
	if err != nil {
		if err == ErrNotFound {
			val := UserQuota{Tokens: r.burst, LastRefill: r.clock.Now()}
			userQuota = val
		} else {
			return false
		}
	}

	userQuota.Tokens += int64(float64(r.clock.Now().Sub(userQuota.LastRefill).Seconds() * r.rate))
	userQuota.LastRefill = r.clock.Now()

	if userQuota.Tokens > r.burst {
		userQuota.Tokens = r.burst
	}

	if userQuota.Tokens > 0 {
		userQuota.Tokens--
		r.store.Set(ctx, key, userQuota)
		return true
	}

	r.store.Set(ctx, key, userQuota)
	return false
}
