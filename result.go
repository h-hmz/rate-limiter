package limiter

import "time"

type Result struct {
	Allowed    bool
	Limit      int64
	Remaining  int64
	RetryAfter time.Duration // maps to Retry-After header; 0 if not applicable
}
