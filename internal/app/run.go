// Package app provides the common process bootstrap used by service commands.
package app

import (
	"context"
	"log/slog"
	"os"

	"github.com/ougi777/metrics-pipeline-go/internal/config"
	"github.com/ougi777/metrics-pipeline-go/internal/logging"
)

// Run loads configuration and emits the process startup event.
func Run(defaultServiceName string) int {
	_, _, exitCode := initialize(defaultServiceName)
	return exitCode
}

// RunService initializes a long-running service and waits for cancellation.
func RunService(ctx context.Context, defaultServiceName string) int {
	_, logger, exitCode := initialize(defaultServiceName)
	if exitCode != 0 {
		return exitCode
	}

	<-ctx.Done()
	logger.Info("service stopped", slog.Any("reason", ctx.Err()))

	return 0
}

func initialize(defaultServiceName string) (config.Config, *slog.Logger, int) {
	cfg, err := config.Load(defaultServiceName)
	if err != nil {
		logger := bootstrapLogger(defaultServiceName)
		logger.Error("configuration failed", slog.Any("error", err))
		return config.Config{}, logger, 1
	}

	logger := logging.New(os.Stdout, cfg.ServiceName, cfg.InstanceID, cfg.LogLevel)
	logger.Info(
		"service initialized",
		slog.String("http_addr", cfg.HTTPAddr),
		slog.String("admin_addr", cfg.AdminAddr),
		slog.Duration("shutdown_timeout", cfg.ShutdownTimeout),
	)

	return cfg, logger, 0
}

func bootstrapLogger(serviceName string) *slog.Logger {
	return logging.New(os.Stderr, serviceName, "bootstrap", "info")
}
