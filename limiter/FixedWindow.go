package limiter

import (
	"context"
	"time"
)

/*
How fixed window works?
Every window of time [1min], user gets X requests.


*/

type FixedWindow struct {
	TokensPerWindow int64
	WindowDuration  time.Duration
}

func NewFixedWindow(tokensPerWindow int64, windowDuration time.Duration) FixedWindow {

	return FixedWindow{
		TokensPerWindow: tokensPerWindow,
		WindowDuration:  windowDuration,
	}
}

func (w *FixedWindow) Allow(ctx context.Context, key string) bool {

}
