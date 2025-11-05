package limiter

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("key not found in storage")

type RateLimiterTokenBucket struct {
	store Storage
	rate  float64 //tokens refill rate per second
	burst int64
}

func NewRateLimiterTokenBucket(rate float64, burst int64, store Storage) *RateLimiterTokenBucket {
	return &RateLimiterTokenBucket{
		store: store,
		rate:  rate,
		burst: burst,
	}
}

func (r *RateLimiterTokenBucket) Allow(ctx context.Context, key string) bool {
	userQuota, err := r.store.Get(ctx, key)
	if err != nil {
		if err == ErrNotFound {
			val := UserQuota{Tokens: r.burst, LastRefill: time.Now()}
			userQuota = val
		} else {
			return false
		}
	}

	userQuota.Tokens += int64(time.Since(userQuota.LastRefill).Seconds() * r.rate)
	userQuota.LastRefill = time.Now()

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
