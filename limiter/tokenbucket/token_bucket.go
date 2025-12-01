package tokenbucket

import (
	"context"
	"errors"
	"time"

	"github.com/h-hmz/rate-limiter/limiter"
)

var ErrNotFound = errors.New("key not found in storage")

type State struct {
	Tokens     int64     `redis:"tokens"` //Note: Redis tags are leaking domain knowledge?
	LastRefill time.Time `redis:"last_refill"`
}

func (s *State) isInitialized() bool {

	if s.LastRefill.IsZero() && s.Tokens == 0 {
		return false
	}
	return true
}

type Limiter struct {
	store Store
	clock limiter.Clock
	rate  float64 //tokens refill rate per second
	burst int64
	ttl   time.Duration
}

func New(rate float64, burst int64, store Store, clock limiter.Clock) *Limiter {
	var ttl time.Duration
	if rate > 0 {
		secondsToFull := float64(burst) / rate
		ttl = time.Duration(secondsToFull * float64(time.Second))
	}

	return &Limiter{
		store: store,
		clock: clock,
		rate:  rate,
		burst: burst,
		ttl:   ttl,
	}
}

func (r *Limiter) Allow(ctx context.Context, key string) (bool, error) {

	_, isAllowed, err := r.store.AtomicUpdate(ctx, key, r.ttl,
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

	if err != nil {
		return false, err
	}

	return isAllowed, nil
}
