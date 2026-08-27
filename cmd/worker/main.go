package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	workerapp "github.com/ougi777/metrics-pipeline-go/internal/app/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(workerapp.Run(ctx))
}
