// Package app 提供各服务命令共用的进程启动逻辑。
package app

import (
	"context"
	"log/slog"
	"os"

	"github.com/ougi777/metrics-pipeline-go/internal/config"
	"github.com/ougi777/metrics-pipeline-go/internal/logging"
)

// Run 读取配置并输出进程启动事件。
func Run(defaultServiceName string) int {
	_, _, exitCode := initialize(defaultServiceName)
	return exitCode
}

// RunService 初始化常驻服务并等待取消信号。
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
