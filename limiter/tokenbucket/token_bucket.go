package tokenbucket

import (
	"context"
	"errors"
	"time"

	"github.com/h-hmz/rate-limiter/limiter"
)

var ErrNotFound = errors.New("key not found in storage")

type State struct {
	Tokens     int64
	LastRefill time.Time
}

type Limiter struct {
	store Store
	clock limiter.Clock
	rate  float64 //tokens refill rate per second
	burst int64
}

func New(rate float64, burst int64, store Store, clock limiter.Clock) *Limiter {
	return &Limiter{
		store: store,
		clock: clock,
		rate:  rate,
		burst: burst,
	}
}

func (r *Limiter) Allow(ctx context.Context, key string) bool {
	_, isAllowed := r.store.AtomicUpdate(ctx, key,
		func() State { //initialization state in case of a new user
			return State{Tokens: r.burst, LastRefill: r.clock.Now()}
		},
		func(userQuota State) (State, bool) {

			userQuota.Tokens += int64(float64(r.clock.Now().Sub(userQuota.LastRefill).Seconds() * r.rate))
			userQuota.LastRefill = r.clock.Now()

			if userQuota.Tokens > r.burst {
				userQuota.Tokens = r.burst
			}

			if userQuota.Tokens > 0 {
				userQuota.Tokens--
				return userQuota, true
			}

			return userQuota, false
		})

	return isAllowed
}
