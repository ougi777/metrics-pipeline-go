// Package api 装配 API 进程的运行时依赖和生命周期。
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	baseapp "github.com/ougi777/metrics-pipeline-go/internal/app"
	"github.com/ougi777/metrics-pipeline-go/internal/config"
	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	"github.com/ougi777/metrics-pipeline-go/internal/messaging"
	"github.com/ougi777/metrics-pipeline-go/internal/service/events"
	"github.com/ougi777/metrics-pipeline-go/internal/service/history"
	ingestservice "github.com/ougi777/metrics-pipeline-go/internal/service/ingest"
	"github.com/ougi777/metrics-pipeline-go/internal/service/summary"
	"github.com/ougi777/metrics-pipeline-go/internal/sse"
	"github.com/ougi777/metrics-pipeline-go/internal/storage/postgres"
	httptransport "github.com/ougi777/metrics-pipeline-go/internal/transport/http"
)

// Run 启动 API 进程并在 context 取消时优雅关闭 HTTP server。
func Run(ctx context.Context) int {
	runtime, exitCode := baseapp.Bootstrap("api")
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
	database, err := postgres.OpenPool(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("postgres initialization failed", slog.Any("error", err))
		return 1
	}
	defer database.Close()

	//创建Metricpoint表持久层dao对象
	store, err := postgres.NewMetricPointStore(database)
	if err != nil {
		logger.Error("metric point store initialization failed", slog.Any("error", err))
		return 1
	}
	hub := sse.NewHub()

	//每个api实例内的广播接收器
	eventBridge, err := messaging.NewRabbitMQMetricEventBridge(messaging.EventBridgeConfig{
		URL:        cfg.AMQPURL,
		InstanceID: runtimeInstanceID(cfg),
	}, messaging.EventSinkFunc(func(ctx context.Context, event domain.RealtimeEvent) error {
		logger.DebugContext(ctx, "realtime metric event received", slog.String("task_id", event.TaskID), slog.Int64("event_seq", event.EventSeq))
		return hub.HandleMetricEvent(ctx, event)
	}), logger)
	if err != nil {
		logger.Error("realtime event bridge initialization failed", slog.Any("error", err))
		return 1
	}

	go func() {
		if err := eventBridge.Run(ctx); err != nil {
			logger.Error("realtime event bridge stopped", slog.Any("error", err))
		}
	}()

	//指标发布到mq
	metricPublisher, err := messaging.NewRabbitMQMetricBatchPublisher(ctx, messaging.PublisherConfig{
		URL:            cfg.AMQPURL,
		Publishers:     cfg.AMQPPublishers,
		WriteTimeout:   cfg.AMQPWriteTimeout,
		ConfirmTimeout: cfg.AMQPConfirmTimeout,
		MaxAttempts:    cfg.AMQPPublishMaxAttempts,
		InitialBackoff: cfg.AMQPPublishInitialBackoff,
		MaxBackoff:     cfg.AMQPPublishMaxBackoff,
	}, logger)
	if err != nil {
		logger.Error("rabbitmq publisher initialization failed", slog.Any("error", err))
		return 1
	}
	defer func() {
		if err := metricPublisher.Close(); err != nil {
			logger.Error("rabbitmq publisher close failed", slog.Any("error", err))
		}
	}()

	ingestService := ingestservice.NewService(metricPublisher)
	// 注册路由
	server := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: httptransport.NewRouter(httptransport.RouterOptions{
			IngestService:  ingestService,
			HistoryService: history.NewService(store),
			SummaryService: summary.NewService(store),
			EventsService:  events.NewService(store),
			EventHub:       hub,
		}),
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server starting", slog.String("addr", cfg.HTTPAddr))
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("http server shutdown failed", slog.Any("error", err))
			return 1
		}
		if err := <-errCh; err != nil {
			logger.Error("http server stopped with error", slog.Any("error", err))
			return 1
		}
		logger.Info("service stopped", slog.Any("reason", ctx.Err()))
		return 0
	case err := <-errCh:
		if err != nil {
			logger.Error("http server failed", slog.Any("error", err))
			return 1
		}
		return 0
	}
}

func runtimeInstanceID(cfg config.Config) string {
	if cfg.InstanceID != "" {
		return cfg.InstanceID
	}
	return "api"
}
