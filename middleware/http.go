package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	limiter "github.com/h-hmz/rate-limiter"
)

type Limiter interface {
	Allow(ctx context.Context, key string) (limiter.Result, error)
}

type KeyExtractor func(r *http.Request) (string, error)

// APIKeyHeaderExtractor extracts a key from a specific header (e.g. "X-API-Key")
func APIKeyHeaderExtractor(headerName string) KeyExtractor {
	return func(r *http.Request) (string, error) {
		key := r.Header.Get(headerName)
		if key == "" {
			return "", fmt.Errorf("missing header: %s", headerName)
		}
		return key, nil
	}
}

func HttpMiddleware(limiter Limiter, keyExtrator KeyExtractor) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			key, err := keyExtrator(r)
			if err != nil {
				http.Error(w, "Rate limiting key missing: "+err.Error(), http.StatusBadRequest)
				return
			}

			result, err := limiter.Allow(r.Context(), key)
			if err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}

			// Decorate the caller's active span. No-op if tracing is not configured.
			span := trace.SpanFromContext(r.Context())
			span.SetAttributes(
				attribute.Bool("ratelimit.allowed", result.Allowed),
				attribute.Int64("ratelimit.limit", result.Limit),
				attribute.Int64("ratelimit.remaining", result.Remaining),
			)

			w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(result.Limit, 10))
			w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(result.Remaining, 10))

			if !result.Allowed {
				span.AddEvent("ratelimit.denied")
				if result.RetryAfter > 0 {
					w.Header().Set("Retry-After", strconv.Itoa(int(result.RetryAfter.Seconds())))
				}
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
