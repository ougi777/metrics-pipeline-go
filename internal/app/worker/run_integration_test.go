//go:build integration

package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ougi777/metrics-pipeline-go/internal/config"
	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	"github.com/ougi777/metrics-pipeline-go/internal/messaging"
)

func TestRunServiceConsumesPersistsAndStops(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_INTEGRATION_URL")
	if databaseURL == "" {
		databaseURL = "postgres://metrics:metrics@localhost:5432/metrics?sslmode=disable"
	}
	amqpURL := os.Getenv("AMQP_INTEGRATION_URL")
	if amqpURL == "" {
		amqpURL = "amqp://metrics:metrics@localhost:5672/"
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping integration database: %v", err)
	}

	taskID := fmt.Sprintf("worker-integration-%d", time.Now().UnixNano())
	cleanupWorkerTask(t, pool, taskID)
	t.Cleanup(func() { cleanupWorkerTask(t, pool, taskID) })

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		done <- runService(runCtx, config.Config{
			DatabaseURL:     databaseURL,
			AMQPURL:         amqpURL,
			ShutdownTimeout: 3 * time.Second,
		}, logger)
	}()

	publisher, err := messaging.NewRabbitMQMetricBatchPublisher(ctx, messaging.PublisherConfig{
		URL:            amqpURL,
		Publishers:     1,
		WriteTimeout:   time.Second,
		ConfirmTimeout: 5 * time.Second,
		MaxAttempts:    10,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
	}, logger)
	if err != nil {
		cancel()
		t.Fatalf("create integration publisher: %v", err)
	}
	defer publisher.Close()

	batch := domain.MetricBatch{
		TaskID: taskID,
		Samples: []domain.MetricSample{{
			Step:            1,
			TimestampMillis: time.Now().UTC().UnixMilli(),
			Metrics:         map[string]float64{"loss": 1.2, "lr": 0.001},
		}},
	}
	if err := publisher.PublishMetricBatch(ctx, batch); err != nil {
		cancel()
		t.Fatalf("publish integration metric batch: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		var points, events, outbox int
		err := pool.QueryRow(ctx, `
SELECT
    (SELECT count(*) FROM metric_points WHERE task_id = $1),
    (SELECT count(*) FROM metric_events WHERE task_id = $1),
    (SELECT count(*) FROM metric_outbox WHERE task_id = $1)
`, taskID).Scan(&points, &events, &outbox)
		if err != nil {
			cancel()
			t.Fatalf("query persisted worker batch: %v", err)
		}
		if points == 2 && events == 1 && outbox == 1 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("persisted counts = points:%d events:%d outbox:%d, want 2/1/1", points, events, outbox)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case exitCode := <-done:
		if exitCode != 0 {
			t.Fatalf("runService() exit code = %d, want 0", exitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after context cancellation")
	}
}

func cleanupWorkerTask(t *testing.T, pool *pgxpool.Pool, taskID string) {
	t.Helper()
	ctx := context.Background()
	for _, statement := range []string{
		"DELETE FROM metric_outbox WHERE task_id = $1",
		"DELETE FROM metric_events WHERE task_id = $1",
		"DELETE FROM metric_points WHERE task_id = $1",
		"DELETE FROM task_event_counters WHERE task_id = $1",
	} {
		if _, err := pool.Exec(ctx, statement, taskID); err != nil {
			t.Fatalf("clean worker integration task %q: %v", taskID, err)
		}
	}
}
