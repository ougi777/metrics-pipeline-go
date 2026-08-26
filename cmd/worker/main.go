package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ougi777/metrics-pipeline-go/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(app.RunService(ctx, "worker"))
}
