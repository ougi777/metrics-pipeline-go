// Package api 装配 API 进程的运行时依赖和生命周期。
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	baseapp "github.com/ougi777/metrics-pipeline-go/internal/app"
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

	return runService(ctx, runtime.Config.HTTPAddr, runtime.Config.ShutdownTimeout, runtime.Logger)
}

func runService(ctx context.Context, httpAddr string, shutdownTimeout time.Duration, logger *slog.Logger) int {
	if ctx.Err() != nil {
		logger.Info("service stopped", slog.Any("reason", ctx.Err()))
		return 0
	}

	metricPublisher := messaging.NoopMetricBatchPublisher{}
	ingestService := ingestservice.NewService(metricPublisher)
	server := &http.Server{
		Addr: httpAddr,
		Handler: httptransport.NewRouter(httptransport.RouterOptions{
			IngestService: ingestService,
		}),
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server starting", slog.String("addr", httpAddr))
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
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
