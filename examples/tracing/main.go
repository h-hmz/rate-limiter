// This example wires up a token bucket limiter backed by Redis, with full
// distributed tracing: the rate-limit decision is attached to the incoming
// request span, and Redis round-trips appear as child spans in the same trace.
//
//	Run it:  docker compose up
//	Jaeger:  http://localhost:16686   (service: "rate-limiter-example")
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	ratelimiter "github.com/h-hmz/rate-limiter"
	rlmiddleware "github.com/h-hmz/rate-limiter/middleware"
	rlstorage "github.com/h-hmz/rate-limiter/storage"
	"github.com/h-hmz/rate-limiter/tokenbucket"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	shutdown, err := initTracer(ctx)
	if err != nil {
		panic(err)
	}
	defer shutdown(context.Background())

	// Enable tracing instrumentation on Redis client
	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := redisotel.InstrumentTracing(client); err != nil {
		panic(err)
	}

	store := rlstorage.NewRedisStore[tokenbucket.State](client)
	limiter := tokenbucket.New(
		1.0, // 1 token per second
		3,   // burst of 3
		store,
		&ratelimiter.WallClock{},
	)

	// HTTP server
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	// Middleware ordering matters: otelhttp must be outermost so the server
	// span exists before the rate-limit middleware reads it via SpanFromContext.
	middlewareHandler := rlmiddleware.HttpMiddleware(
		limiter,
		rlmiddleware.APIKeyHeaderExtractor("X-API-Key"),
	)(handler)

	mux := http.NewServeMux()
	mux.Handle("/api", otelhttp.NewHandler(middlewareHandler, "GET /api"))

	fmt.Println("listening on :8080")
	fmt.Println("  curl -H 'X-API-Key: user1' http://localhost:8080/api")
	fmt.Println("  Jaeger UI: http://localhost:16686")

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
}

// initTracer configures an OTLP/HTTP trace exporter and installs a global
// TracerProvider. Returns a shutdown func that flushes pending spans.
func initTracer(ctx context.Context) (func(context.Context) error, error) {
	endpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")

	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("rate-limiter-example"),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
