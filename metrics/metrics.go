// Package metrics provides an optional observability decorator for any rate limiter
// that satisfies the Limiter interface.
//
// It records two OTel metrics:
//   - ratelimit.requests.total  (counter): every Allow() call, labeled by outcome
//   - ratelimit.latency.seconds (histogram): end-to-end Allow() duration
package metrics

import (
	"context"
	"time"

	limiter "github.com/h-hmz/rate-limiter"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// InstrumentedLimiter wraps any Limiter and records OTel metrics on every call.
type InstrumentedLimiter struct {
	limiter.Limiter

	// requestsTotal counts every Allow() call.
	requestsTotal metric.Int64Counter

	// latency records the duration of each Allow() call in seconds.
	latency metric.Float64Histogram
}

// New creates an InstrumentedLimiter around the given limiter.
func New(inner limiter.Limiter) (*InstrumentedLimiter, error) {
	meter := otel.Meter("ratelimiter")

	requestsTotal, err := meter.Int64Counter(
		"ratelimit.requests.total",
		metric.WithDescription("Total number of rate limit decisions"),
	)
	if err != nil {
		return nil, err
	}

	latency, err := meter.Float64Histogram(
		"ratelimit.latency.seconds",
		metric.WithDescription("Duration of rate limit evaluations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.0000001, 0.0000005, 0.000001, 0.000005, // 100ns, 500ns, 1us, 5us    (in-memory range)
			0.00001, 0.00005, 0.0001, 0.0005, // 10us, 50us, 100us, 500us  (Redis range)
			0.001, 0.01, 0.1, // 1ms, 10ms, 100ms          (degraded/timeout)
		),
	)
	if err != nil {
		return nil, err
	}

	return &InstrumentedLimiter{
		Limiter:       inner,
		requestsTotal: requestsTotal,
		latency:       latency,
	}, nil
}

// Allow delegates to the wrapped limiter and records metrics around the call.
func (l *InstrumentedLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	start := time.Now()

	result, err := l.Limiter.Allow(ctx, key)

	// Record latency regardless of error, a slow failure is still interesting from an observability perspective.
	l.latency.Record(ctx, time.Since(start).Seconds())

	if err != nil {
		l.requestsTotal.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("outcome", "error"),
			),
		)
		return result, err
	}

	outcome := "denied"
	if result.Allowed {
		outcome = "allowed"
	}
	l.requestsTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("outcome", outcome),
		),
	)

	return result, nil
}
