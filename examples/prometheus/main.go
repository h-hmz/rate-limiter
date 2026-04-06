// This example wires up a token bucket limiter with the metrics decorator
// and exposes a Prometheus-scrapeable /metrics endpoint.
//
//	Run it: go run ./examples/prometheus
package main

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	ratelimiter "github.com/h-hmz/rate-limiter"
	rlmetrics "github.com/h-hmz/rate-limiter/metrics"
	rlmiddleware "github.com/h-hmz/rate-limiter/middleware"
	rlstorage "github.com/h-hmz/rate-limiter/storage"
	"github.com/h-hmz/rate-limiter/tokenbucket"
)

func main() {
	// OTel setup: Prometheus exporter
	exporter, err := promexporter.New()
	if err != nil {
		panic(err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	otel.SetMeterProvider(provider)

	// Rate limiter setup
	store := rlstorage.NewInMemoryStore[tokenbucket.State](&ratelimiter.WallClock{})
	limiter := tokenbucket.New(
		1.0, // 1 token per second
		3,   // burst of 3
		store,
		&ratelimiter.WallClock{},
	)

	instrumented, err := rlmetrics.New(limiter)
	if err != nil {
		panic(err)
	}

	// HTTP server
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "OK")
	})

	mux := http.NewServeMux()
	mux.Handle("/api", rlmiddleware.HttpMiddleware(
		instrumented,
		rlmiddleware.APIKeyHeaderExtractor("X-API-Key"),
	)(handler))
	mux.Handle("/metrics", promhttp.Handler())

	fmt.Println("listening on :8080")
	fmt.Println("  curl -H 'X-API-Key: user1' http://localhost:8080/api")
	fmt.Println("  curl http://localhost:8080/metrics | grep ratelimit")
	http.ListenAndServe(":8080", mux)
}
