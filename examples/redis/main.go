// This example wires up a token bucket limiter backed by Redis.
//
//	Run it: docker compose up
package main

import (
	"fmt"
	"net/http"

	ratelimiter "github.com/h-hmz/rate-limiter"
	rlmiddleware "github.com/h-hmz/rate-limiter/middleware"
	rlstorage "github.com/h-hmz/rate-limiter/storage"
	"github.com/h-hmz/rate-limiter/tokenbucket"
)

func main() {
	redisAddr := "localhost:6379"

	store := rlstorage.NewRedisStore[tokenbucket.State](redisAddr)
	limiter := tokenbucket.New(
		1.0, // 1 token per second
		3,   // burst of 3
		store,
		&ratelimiter.WallClock{},
	)

	// HTTP server
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "OK")
	})

	mux := http.NewServeMux()
	mux.Handle("/api", rlmiddleware.HttpMiddleware(
		limiter,
		rlmiddleware.APIKeyHeaderExtractor("X-API-Key"),
	)(handler))

	fmt.Println("listening on :8080 (redis:", redisAddr+")")
	fmt.Println("  curl -H 'X-API-Key: user1' http://localhost:8080/api")
	http.ListenAndServe(":8080", mux)
}
