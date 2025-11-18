package fixedwindow

import (
	"context"
	"errors"
	"time"

	"github.com/h-hmz/rate-limiter/limiter"
)

var ErrNotFound = errors.New("key not found in storage")

type State struct {
	RemainingTokens int64 `redis:"remaining_tokens"`
	LastWindowID    int64 `redis:"last_window_id"`
}

type Limiter struct {
	store           Store
	clock           limiter.Clock
	TokensPerWindow int64
	WindowStart     time.Time
	WindowDuration  time.Duration
}

func New(tokensPerWindow int64, windowDuration time.Duration, store Store, clock limiter.Clock) Limiter {
	return Limiter{
		store:           store,
		clock:           clock,
		TokensPerWindow: tokensPerWindow,
		WindowDuration:  windowDuration,
	}
}

func (r *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	currentTimeNano := r.clock.Now().UnixNano()
	currentWindowID := currentTimeNano / r.WindowDuration.Nanoseconds()

	_, isAllowed, err := r.store.AtomicUpdate(ctx, key,
		func() State { //initialization state in case of a new user
			return State{RemainingTokens: r.TokensPerWindow, LastWindowID: currentWindowID}
		},
		func(userQuota State) (State, bool) {

			if userQuota.LastWindowID < currentWindowID {
				userQuota.LastWindowID = currentWindowID
				userQuota.RemainingTokens = r.TokensPerWindow
			}

			if userQuota.RemainingTokens > 0 {
				userQuota.RemainingTokens--
				return userQuota, true
			}

			return userQuota, false
		})

	return isAllowed, err
}
