package snoop

import (
	"time"
)

type RateLimiterTokenBucketInMemory struct {
	usersMap map[string]UserQuota
	rate     float64 //tokens refill rate per second
	burst    int64
}

func NewRateLimiterTokenBucketInMemory(rate float64, burst int64) *RateLimiterTokenBucketInMemory {
	return &RateLimiterTokenBucketInMemory{
		usersMap: make(map[string]UserQuota),
		rate:     rate,
		burst:    burst,
	}
}

func (r *RateLimiterTokenBucketInMemory) Allow(key string) bool {
	_, ok := r.usersMap[key]

	if !ok { //create user
		r.usersMap[key] = UserQuota{r.burst, time.Now()}
	}

	userQuota, _ := r.usersMap[key]

	userQuota.tokens += int64(time.Since(userQuota.lastRefill).Seconds() * r.rate)
	userQuota.lastRefill = time.Now()

	if userQuota.tokens > r.burst {
		userQuota.tokens = r.burst
	}

	if userQuota.tokens > 0 {
		userQuota.tokens--
		r.usersMap[key] = userQuota
		return true
	}

	r.usersMap[key] = userQuota
	return false
}
