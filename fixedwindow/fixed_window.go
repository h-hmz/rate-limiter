package fixedwindow

import (
	"context"
	"time"

	limiter "github.com/h-hmz/rate-limiter"
	"github.com/h-hmz/rate-limiter/storage"
)

type State struct {
	RemainingTokens int64 `redis:"remaining_tokens"`
	LastWindowID    int64 `redis:"last_window_id"`
}

type Limiter struct {
	store           storage.Store[State]
	clock           limiter.Clock
	TokensPerWindow int64
	WindowDuration  time.Duration
	ttl             time.Duration
}

func New(tokensPerWindow int64, windowDuration time.Duration, store storage.Store[State], clock limiter.Clock) *Limiter {
	return &Limiter{
		store:           store,
		clock:           clock,
		TokensPerWindow: tokensPerWindow,
		WindowDuration:  windowDuration,
		ttl:             windowDuration,
	}
}

func (r *Limiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	currentTimeNano := r.clock.Now().UnixNano()
	currentWindowID := currentTimeNano / r.WindowDuration.Nanoseconds()

	state, isAllowed, err := r.store.AtomicUpdate(ctx, key, r.ttl,
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

	if err != nil {
		return limiter.Result{}, err
	}

	nextWindowNano := (currentWindowID + 1) * r.WindowDuration.Nanoseconds()
	retryAfter := time.Duration(nextWindowNano - currentTimeNano)

	return limiter.Result{
		Allowed:    isAllowed,
		Limit:      r.TokensPerWindow,
		Remaining:  state.RemainingTokens,
		RetryAfter: retryAfter,
	}, nil
}
