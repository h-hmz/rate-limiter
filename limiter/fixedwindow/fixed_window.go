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

	state, err := r.store.Get(ctx, key)
	if err != nil {
		if err == ErrNotFound {
			val := State{remainingTokens: r.TokensPerWindow, lastWindowID: currentWindowID}
			state = val
		} else {
			return false
		}
	}

	if state.lastWindowID < currentWindowID {
		state.lastWindowID = currentWindowID
		state.remainingTokens = r.TokensPerWindow
	}

	if state.remainingTokens > 0 {
		state.remainingTokens--
		err = r.store.Set(ctx, key, state)
		if err != nil {
			return false
		}
		return true
	}
	err = r.store.Set(ctx, key, state)
	if err != nil {
		return false
	}
	return false
}
