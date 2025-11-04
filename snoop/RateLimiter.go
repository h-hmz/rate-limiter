package snoop

import "time"

type UserQuota struct {
	tokens     int64
	lastRefill time.Time
}
