package snoop

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RateLimiterTokenBucketRedis struct {
	client *redis.Client
	rate   float64 //tokens refill rate per second
	burst  int64
}

func NewRateLimiterTokenBucketRedis(rate float64, burst int64) *RateLimiterTokenBucketRedis {
	return &RateLimiterTokenBucketRedis{
		client: redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		}),
		rate:  rate,
		burst: burst,
	}
}

func (r *RateLimiterTokenBucketRedis) Allow(ctx context.Context, key string) (bool, error) {

	vals, err := r.client.HMGet(ctx, key, "tokens", "last_refill").Result()
	if err != nil {

		if err == redis.Nil { // create user
			r.client.HMSet(ctx, key,
				"tokens", r.burst,
				"lastRefill", time.Now().UnixNano(),
			)
		} else {
			return false, err
		}
	}

	tokens, _ := strconv.ParseInt(vals[0].(string), 10, 64)

	lastRefillUnixNano, _ := strconv.ParseInt(vals[1].(string), 10, 64)
	lastRefill := time.Unix(0, lastRefillUnixNano)

	tokens += int64(time.Since(lastRefill).Seconds() * r.rate)
	lastRefill = time.Now()

	if tokens > r.burst {
		tokens = r.burst
	}

	if tokens > 0 {
		tokens--
		r.client.HMSet(ctx, key,
			"tokens", tokens,
			"lastRefill", lastRefill.UnixNano(),
		)
		return true, nil
	}

	return false, nil
}
