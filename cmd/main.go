// cmd/main.go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"diagnostic-agent/internal/agent"
)

func main() {
	hostAddr := os.Getenv("HOST_ADDR")
	if hostAddr == "" {
		hostAddr = "localhost:8080"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize agent
	a, err := agent.New(hostAddr, redisAddr)
	if err != nil {
		log.Fatalf("Failed to initialize agent: %v", err)
	}

	// Handle shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	// Run agent
	if err := a.Run(ctx); err != nil {
		log.Fatalf("Agent error: %v", err)
	}
}
