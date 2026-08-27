package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	apiapp "github.com/ougi777/metrics-pipeline-go/internal/app/api"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(apiapp.Run(ctx))
}
