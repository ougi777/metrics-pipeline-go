// Package api 装配 API 进程的运行时依赖和生命周期。
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	baseapp "github.com/ougi777/metrics-pipeline-go/internal/app"
	"github.com/ougi777/metrics-pipeline-go/internal/config"
	"github.com/ougi777/metrics-pipeline-go/internal/messaging"
	ingestservice "github.com/ougi777/metrics-pipeline-go/internal/service/ingest"
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
	server := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: httptransport.NewRouter(httptransport.RouterOptions{
			IngestService: ingestService,
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
