// Package worker 装配 worker 进程的运行时依赖和生命周期。
package worker

import (
	"context"
	"log/slog"

	baseapp "github.com/ougi777/metrics-pipeline-go/internal/app"
	"github.com/ougi777/metrics-pipeline-go/internal/config"
	"github.com/ougi777/metrics-pipeline-go/internal/messaging"
	"github.com/ougi777/metrics-pipeline-go/internal/storage/postgres"
	"golang.org/x/sync/errgroup"
)

// Run 启动 worker 进程。
func Run(ctx context.Context) int {
	runtime, exitCode := baseapp.Bootstrap("worker")
	if exitCode != 0 {
		return exitCode
	}

	return runService(ctx, runtime.Config, runtime.Logger)
}

func runService(ctx context.Context, cfg config.Config, logger *slog.Logger) int {
	if ctx.Err() != nil {
		logger.Info("service stopped", slog.Any("reason", ctx.Err()))
		return 0
	}

	pool, err := postgres.OpenPool(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("postgres initialization failed", slog.Any("error", err))
		return 1
	}
	defer pool.Close()
	if err := postgres.EnsureWorkerDailyPartitions(ctx, pool); err != nil {
		logger.Error("postgres partition initialization failed", slog.Any("error", err))
		return 1
	}

	store, err := postgres.NewMetricPointStore(pool)
	if err != nil {
		logger.Error("metric point store initialization failed", slog.Any("error", err))
		return 1
	}
	consumer, err := messaging.NewRabbitMQMetricConsumer(messaging.ConsumerConfig{
		URL:             cfg.AMQPURL,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}, store, logger)
	if err != nil {
		logger.Error("rabbitmq consumer initialization failed", slog.Any("error", err))
		return 1
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return postgres.RunDailyPartitionMaintenance(groupCtx, pool, logger)
	})
	group.Go(func() error {
		logger.Info("worker consumer starting")
		return consumer.Run(groupCtx)
	})
	if err := group.Wait(); err != nil {
		logger.Error("worker runtime failed", slog.Any("error", err))
		return 1
	}
	logger.Info("service stopped", slog.Any("reason", ctx.Err()))

	return 0
}
