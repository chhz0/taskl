package main

import (
	"context"
	"fmt"
	"time"

	"github.com/chhz0/taskl/core"
	"github.com/chhz0/taskl/middleware"
	"github.com/chhz0/taskl/retry"
	"github.com/chhz0/taskl/server"
	"github.com/chhz0/taskl/storage"
	"github.com/chhz0/taskl/types"
)

func main() {
	store := storage.NewRedisStorage("localhost:6379", "", 0)

	cfg := server.Config{
		HTTPAddr:       ":8080",
		QueueSize:      100,
		StorageBackend: store,
		WorkerCount:    10,
	}

	srv, err := server.NewServer(cfg)
	if err != nil {
		panic(err)
	}

	registry := core.NewTaskRegistry()
	handlerChain := middleware.Chain(
		middleware.Logger(),
		middleware.Timeout(10*1000),
		middleware.Metrics(),
	)

	registry.Register("email", core.TaskHandler(handlerChain(
		func(ctx context.Context, task *types.Task) error {
			fmt.Println("Sending email...")
			return nil
		},
	)))

	_ = retry.NewRetryManager(&retry.ExponentialBackoff{
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		MaxAttempts:  5,
	})

	if err := srv.Start(); err != nil {
		panic(err)
	}
}
