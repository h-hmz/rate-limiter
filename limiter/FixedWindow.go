package limiter

import (
	"context"
	"time"
)

type FixedWindowState struct {
	remainingTokens int64
	lastWindowID    int64
}

type FixedWindow struct {
	store           FixedWindowStore
	clock           Clock
	TokensPerWindow int64
	WindowStart     time.Time
	WindowDuration  time.Duration
}

func NewFixedWindow(tokensPerWindow int64, windowDuration time.Duration, store FixedWindowStore, clock Clock) FixedWindow {
	return FixedWindow{
		store:           store,
		clock:           clock,
		TokensPerWindow: tokensPerWindow,
		WindowDuration:  windowDuration,
	}
}

func (r *FixedWindow) Allow(ctx context.Context, key string) bool {

	currentTimeNano := r.clock.Now().UnixNano()
	currentWindowID := currentTimeNano / r.WindowDuration.Nanoseconds()

	state, err := r.store.Get(ctx, key)
	if err != nil {
		if err == ErrNotFound {
			val := FixedWindowState{remainingTokens: r.TokensPerWindow, lastWindowID: currentWindowID}
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
