// Package app provides the common process bootstrap used by service commands.
package app

import (
	"log/slog"
	"os"

	"github.com/ougi777/metrics-pipeline-go/internal/config"
	"github.com/ougi777/metrics-pipeline-go/internal/logging"
)

// Run loads configuration and emits the process startup event.
func Run(defaultServiceName string) int {
	cfg, err := config.Load(defaultServiceName)
	if err != nil {
		bootstrapLogger(defaultServiceName).Error("configuration failed", slog.Any("error", err))
		return 1
	}

	logger := logging.New(os.Stdout, cfg.ServiceName, cfg.InstanceID, cfg.LogLevel)
	logger.Info(
		"service initialized",
		slog.String("http_addr", cfg.HTTPAddr),
		slog.String("admin_addr", cfg.AdminAddr),
		slog.Duration("shutdown_timeout", cfg.ShutdownTimeout),
	)

	return 0
}

func bootstrapLogger(serviceName string) *slog.Logger {
	return logging.New(os.Stderr, serviceName, "bootstrap", "info")
}
