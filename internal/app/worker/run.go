// Package worker 装配 worker 进程的运行时依赖和生命周期。
package worker

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	baseapp "github.com/ougi777/metrics-pipeline-go/internal/app"
	"github.com/ougi777/metrics-pipeline-go/internal/config"
	"github.com/ougi777/metrics-pipeline-go/internal/health"
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

	//创建pg连接池
	pool, err := postgres.OpenPool(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("postgres initialization failed", slog.Any("error", err))
		return 1
	}
	defer pool.Close()

	//预创建日分区
	if err := postgres.EnsureWorkerDailyPartitions(ctx, pool); err != nil {
		logger.Error("postgres partition initialization failed", slog.Any("error", err))
		return 1
	}

	//创建持久层操作对象：
	// 1.指标持久化 2.历史查询 3.摘要查询
	store, err := postgres.NewMetricPointStore(pool)
	if err != nil {
		logger.Error("metric point store initialization failed", slog.Any("error", err))
		return 1
	}

	//创建outbox relay，轮询未发送到mq的消息发布到mq exchange中
	outboxStore, err := postgres.NewOutboxStore(pool)
	if err != nil {
		logger.Error("outbox store initialization failed", slog.Any("error", err))
		return 1
	}
	eventPublisher, err := messaging.NewRabbitMQMetricEventPublisher(messaging.MetricEventPublisherConfig{
		URL:            cfg.AMQPURL,
		ConfirmTimeout: cfg.AMQPConfirmTimeout, //broker confirm超时时间
	})
	if err != nil {
		logger.Error("realtime publisher initialization failed", slog.Any("error", err))
		return 1
	}
	defer func() { _ = eventPublisher.Close() }()

	//聚合outbox表操作、消息发布到mq的集合对象
	relay, err := messaging.NewOutboxRelay(messaging.OutboxRelayConfig{}, outboxStore, eventPublisher, logger)
	if err != nil {
		logger.Error("outbox relay initialization failed", slog.Any("error", err))
		return 1
	}

	//mq指标消费者，取出mq消息批量落库
	consumer, err := messaging.NewRabbitMQMetricConsumer(messaging.ConsumerConfig{
		URL:             cfg.AMQPURL,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}, store, logger)
	if err != nil {
		logger.Error("rabbitmq consumer initialization failed", slog.Any("error", err))
		return 1
	}
	state := health.NewState()
	adminServer := &http.Server{Addr: cfg.AdminAddr, Handler: health.Handler{
		State: state, ProbeTimeout: 2 * time.Second,
		Postgres: func(ctx context.Context) error { return pool.Ping(ctx) },
		RabbitMQ: func(ctx context.Context) error { return messaging.Ping(ctx, cfg.AMQPURL) },
	}}
	adminErrCh := make(chan error, 1)
	go func() {
		logger.Info("admin server starting", slog.String("addr", cfg.AdminAddr))
		if err := adminServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			adminErrCh <- err
		}
	}()
	state.SetReady(true)

	//errorgroup 启动：
	// 1.分区清理维护协程
	// 2.批量落库协程
	// 3.轮询outbox广播sse消息
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return postgres.RunPartitionMaintenance(
			groupCtx,
			pool,
			logger,
			cfg.RetentionWindow,
			cfg.PartitionMaintenanceInterval,
			10000,
		)
	})
	group.Go(func() error {
		logger.Info("worker consumer starting")
		return consumer.Run(groupCtx)
	})
	group.Go(func() error {
		logger.Info("outbox relay starting")
		return relay.Run(groupCtx)
	})
	groupDone := make(chan error, 1)
	go func() { groupDone <- group.Wait() }()
	select {
	case <-ctx.Done():
		state.SetReady(false)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := adminServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("admin server shutdown failed", slog.Any("error", err))
			return 1
		}
		if err := <-groupDone; err != nil {
			logger.Error("worker runtime failed", slog.Any("error", err))
			return 1
		}
	case err := <-groupDone:
		state.SetReady(false)
		_ = adminServer.Close()
		if err != nil {
			logger.Error("worker runtime failed", slog.Any("error", err))
			return 1
		}
	case err := <-adminErrCh:
		state.SetReady(false)
		if err != nil {
			logger.Error("admin server failed", slog.Any("error", err))
			return 1
		}
	}
	logger.Info("service stopped", slog.Any("reason", ctx.Err()))

	return 0
}
