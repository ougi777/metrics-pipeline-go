// Package app 提供各服务命令共用的启动基础设施。
package app

import (
	"context"
	"log/slog"
	"os"

	"github.com/ougi777/metrics-pipeline-go/internal/config"
	"github.com/ougi777/metrics-pipeline-go/internal/logging"
)

// Runtime 保存服务启动后会被各进程装配层复用的公共对象。
type Runtime struct {
	Config config.Config
	Logger *slog.Logger
}

// Bootstrap 读取配置、初始化日志，并返回进程装配所需的公共运行时对象。
func Bootstrap(defaultServiceName string) (Runtime, int) {
	cfg, err := config.Load(defaultServiceName)
	if err != nil {
		logger := bootstrapLogger(defaultServiceName)
		logger.Error("configuration failed", slog.Any("error", err))
		return Runtime{Logger: logger}, 1
	}

	logger := logging.New(os.Stdout, cfg.ServiceName, cfg.InstanceID, cfg.LogLevel)
	logger.Info(
		"service initialized",
		slog.String("http_addr", cfg.HTTPAddr),
		slog.String("admin_addr", cfg.AdminAddr),
		slog.Duration("shutdown_timeout", cfg.ShutdownTimeout),
	)

	return Runtime{Config: cfg, Logger: logger}, 0
}

// WaitForCancel 是尚未接入后台工作循环的常驻服务占位启动逻辑。
func WaitForCancel(ctx context.Context, logger *slog.Logger) int {
	<-ctx.Done()
	logger.Info("service stopped", slog.Any("reason", ctx.Err()))

	return 0
}

func bootstrapLogger(serviceName string) *slog.Logger {
	return logging.New(os.Stderr, serviceName, "bootstrap", "info")
}
