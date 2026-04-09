# rate-limiter

A production-oriented rate limiting library in Go, built to showcase clean abstraction design, concurrency safety, and storage portability.

Two algorithms are implemented: **Fixed Window** and **Token Bucket**. Both backed by the same generic storage interface, with in-memory and Redis backends that are fully interchangeable.

## Usage

A typical wiring in your application:

```go
import (
    "net/http"
    "os"
    "time"

    "github.com/redis/go-redis/v9"

    ratelimiter  "github.com/h-hmz/rate-limiter"
    rlmiddleware "github.com/h-hmz/rate-limiter/middleware"
    rlstorage    "github.com/h-hmz/rate-limiter/storage"
    "github.com/h-hmz/rate-limiter/tokenbucket"
)

client := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})
store := rlstorage.NewRedisStore[tokenbucket.State](client)
limiter := tokenbucket.New(
    10.0,   // refill rate: tokens per second
    50,     // burst capacity
    store,
    &ratelimiter.WallClock{},
)

http.Handle("/api/", rlmiddleware.HttpMiddleware(
    limiter,
    rlmiddleware.APIKeyHeaderExtractor("X-API-Key"),
)(myHandler))
```

Swap `rlstorage.NewRedisStore` for `rlstorage.NewInMemoryStore` for single-instance deployments, or swap `tokenbucket` for `fixedwindow`. The middleware and the rest of the wiring stay the same.

## Features

- **Two algorithms**: Fixed Window and Token Bucket
- **Two backends**: in-memory (sharded map) and Redis (optimistic locking via WATCH/MULTI/EXEC)
- **Storage-portable**: swap backends without changing algorithm code
- **Concurrency-safe**: sharded locking for in-memory, atomic transactions for Redis
- **Testable**: injectable `Clock` interface enables deterministic time-based tests
- **HTTP middleware** included, with a composable `KeyExtractor` pattern

## Algorithms

### Fixed Window

Limits requests to `N` per time window. The window resets on a fixed schedule (e.g. every 60s from epoch), not from first request.

```go
store := storage.NewInMemoryStore[fixedwindow.State](clock)
limiter := fixedwindow.New(
    100,           // tokens per window
    time.Minute,   // window duration
    store,
    &limiter.WallClock{},
)

allowed, err := limiter.Allow(ctx, "user-id")
```

### Token Bucket

Tokens refill continuously at a fixed rate up to a burst capacity. Allows short bursts while enforcing a long-term average rate.

```go
store := storage.NewInMemoryStore[tokenbucket.State](clock)
limiter := tokenbucket.New(
    10.0,          // refill rate: tokens per second
    100,           // burst capacity
    store,
    &limiter.WallClock{},
)

allowed, err := limiter.Allow(ctx, "user-id")
```

## Backends

### In-Memory

Uses a sharded map (256 shards, xxhash) to minimize lock contention. Supports TTL with lazy expiration and an optional background GC.

```go
store := storage.NewInMemoryStore[fixedwindow.State](&limiter.WallClock{})

// Optional: run background GC every 5 minutes
store.StartGC(5 * time.Minute)
```

### Redis

Uses optimistic locking (WATCH/MULTI/EXEC) with retry. Suitable for distributed deployments where multiple instances share state.

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
store := storage.NewRedisStore[fixedwindow.State](client)
```

The `*redis.Client` is injected by the caller (rather than constructed inside the store) so that TLS, pool sizing, timeouts, sentinel/cluster topology, and tracing instrumentation (`redisotel.InstrumentTracing(client)`) all remain under the application's control.

> The Redis backend stores algorithm state as a Redis hash, using struct field tags (`redis:"..."`) for mapping.

## HTTP Middleware

```go
limiterInstance := fixedwindow.New(100, time.Minute, store, &limiter.WallClock{})

mux := http.NewServeMux()
mux.Handle("/api/", middleware.HttpMiddleware(
    limiterInstance,
    middleware.APIKeyHeaderExtractor("X-API-Key"),
)(myHandler))
```

`KeyExtractor` is a plain function type. It easy to replace with IP-based, JWT-based, or any other extraction logic.

## Metrics (OpenTelemetry)

The `metrics` package provides an optional decorator that records OTel metrics around any limiter. The core library has no observability dependency so consumers who don't need metrics never import this package.

```go
import (
    rlmetrics "github.com/h-hmz/rate-limiter/metrics"
)

// Works with both tokenbucket and fixedwindow.
instrumented, err := rlmetrics.New(limiter)

// Use it anywhere you'd use the original limiter.
http.Handle("/api/", rlmiddleware.HttpMiddleware(
    instrumented,
    rlmiddleware.APIKeyHeaderExtractor("X-API-Key"),
)(myHandler))
```

This records two metrics:
- `ratelimit.requests.total`: counter labeled by `outcome` (`allowed`, `denied`, `error`)
- `ratelimit.latency.seconds`: histogram of `Allow()` duration

The decorator uses OTel's API but does not configure an exporter. That's your application's responsibility. See `/examples` for examples using configured exporters.

## Tracing

The HTTP middleware decorates the caller's active span with `ratelimit.allowed`, `ratelimit.limit`, and `ratelimit.remaining` attributes, and emits a `ratelimit.denied` event on a 429. If tracing is not configured, this is a no-op.

Since the caller owns the `*redis.Client`, you can attach `redisotel.InstrumentTracing(client)` to have Redis round-trips show up as child spans of the incoming request.