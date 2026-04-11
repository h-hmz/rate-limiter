package limiter

import "time"

type Clock interface {
	Now() time.Time
}

type WallClock struct{}

func (c *WallClock) Now() time.Time {
	return time.Now()
}

type MockClock struct {
	now time.Time
}

func NewMockClock(start time.Time) *MockClock {
	return &MockClock{now: start}
}

func (c *MockClock) Now() time.Time {
	return c.now
}

func (c *MockClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}
