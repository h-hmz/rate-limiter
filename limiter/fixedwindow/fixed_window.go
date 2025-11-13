package fixedwindow

import (
	"context"
	"errors"
	"time"

	"github.com/h-hmz/rate-limiter/limiter"
)

var ErrNotFound = errors.New("key not found in storage")

type State struct {
	remainingTokens int64
	lastWindowID    int64
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

func (r *Limiter) Allow(ctx context.Context, key string) bool {
	currentTimeNano := r.clock.Now().UnixNano()
	currentWindowID := currentTimeNano / r.WindowDuration.Nanoseconds()

	_, isAllowed := r.store.AtomicUpdate(ctx, key,
		func() State { //initialization state in case of a new user
			return State{remainingTokens: r.TokensPerWindow, lastWindowID: currentWindowID}
		},
		func(userQuota State) (State, bool) {

			if userQuota.lastWindowID < currentWindowID {
				userQuota.lastWindowID = currentWindowID
				userQuota.remainingTokens = r.TokensPerWindow
			}

			if userQuota.remainingTokens > 0 {
				userQuota.remainingTokens--
				return userQuota, true
			}

			return userQuota, false
		})

	return isAllowed
}
