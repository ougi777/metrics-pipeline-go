//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	"github.com/ougi777/metrics-pipeline-go/internal/messaging"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestMetricPointStorePersistsAndDeduplicatesFlush(t *testing.T) {
	pool, store := newMetricStoreIntegrationDatabase(t)
	ctx := context.Background()
	points := []domain.MetricPoint{
		{TaskID: "task-a", Key: "loss", Step: 1, TimestampMillis: time.Now().UTC().UnixMilli(), Value: 1.2},
		{TaskID: "task-a", Key: "lr", Step: 1, TimestampMillis: time.Now().UTC().UnixMilli(), Value: 0.001},
		{TaskID: "task-b", Key: "loss", Step: 1, TimestampMillis: time.Now().UTC().UnixMilli(), Value: 2.3},
	}

	if err := store.Flush(ctx, points); err != nil {
		t.Fatalf("first Flush() error = %v", err)
	}
	assertTableCount(t, pool, "metric_points", 3)
	assertTableCount(t, pool, "metric_events", 2)
	assertTableCount(t, pool, "metric_outbox", 2)
	assertCounter(t, pool, "task-a", 1)
	assertCounter(t, pool, "task-b", 1)

	if err := store.Flush(ctx, points); err != nil {
		t.Fatalf("duplicate Flush() error = %v", err)
	}
	assertTableCount(t, pool, "metric_points", 3)
	assertTableCount(t, pool, "metric_events", 2)
	assertTableCount(t, pool, "metric_outbox", 2)
	assertCounter(t, pool, "task-a", 1)

	newPoint := domain.MetricPoint{
		TaskID:          "task-a",
		Key:             "accuracy",
		Step:            2,
		TimestampMillis: time.Now().UTC().UnixMilli(),
		Value:           0.95,
	}
	if err := store.Flush(ctx, append([]domain.MetricPoint{points[0]}, newPoint)); err != nil {
		t.Fatalf("partially duplicate Flush() error = %v", err)
	}
	assertTableCount(t, pool, "metric_points", 4)
	assertTableCount(t, pool, "metric_events", 3)
	assertTableCount(t, pool, "metric_outbox", 3)
	assertCounter(t, pool, "task-a", 2)

	var payload []byte
	if err := pool.QueryRow(ctx, `
SELECT payload
FROM metric_events
WHERE task_id = 'task-a' AND event_seq = 2
`).Scan(&payload); err != nil {
		t.Fatalf("read second event payload: %v", err)
	}
	var event struct {
		Points []map[string]any `json:"points"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode event payload: %v", err)
	}
	if len(event.Points) != 1 || event.Points[0]["accuracy"] != 0.95 {
		t.Fatalf("second event payload = %s, want only newly inserted accuracy point", payload)
	}
}

func TestMetricPointStorePreservesReservedEventFields(t *testing.T) {
	pool, store := newMetricStoreIntegrationDatabase(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	points := []domain.MetricPoint{
		{TaskID: "reserved-fields", Key: "step", Step: 7, TimestampMillis: now.UnixMilli(), Value: 91},
		{TaskID: "reserved-fields", Key: "ts", Step: 7, TimestampMillis: now.UnixMilli(), Value: 92},
	}

	if err := store.Flush(ctx, points); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM metric_events WHERE task_id = 'reserved-fields'`).Scan(&payload); err != nil {
		t.Fatalf("read reserved-fields event payload: %v", err)
	}
	var event struct {
		Points []map[string]any `json:"points"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode reserved-fields event payload: %v", err)
	}
	if len(event.Points) != 1 || event.Points[0]["step"] != float64(7) || event.Points[0]["ts"] != float64(now.UnixMilli()) {
		t.Fatalf("event payload = %s, want metadata step=7 and ts=%d", payload, now.UnixMilli())
	}
}

func TestEnsureDailyPartitionsAllowsFutureFlush(t *testing.T) {
	pool, store := newMetricStoreIntegrationDatabase(t)
	ctx := context.Background()
	future := time.Now().UTC().AddDate(0, 0, 10)

	if err := store.Flush(ctx, []domain.MetricPoint{{
		TaskID:          "future-partition",
		Key:             "loss",
		Step:            1,
		TimestampMillis: future.UnixMilli(),
		Value:           1.2,
	}}); err != nil {
		t.Fatalf("future Flush() error = %v", err)
	}
	assertTableCount(t, pool, "metric_points", 1)
}

func TestMetricPointStoreRollsBackAllTablesOnFailure(t *testing.T) {
	pool, store := newMetricStoreIntegrationDatabase(t)
	ctx := context.Background()
	now := time.Now().UTC().UnixMilli()
	points := []domain.MetricPoint{
		{TaskID: "rollback-task", Key: "loss", Step: 1, TimestampMillis: now, Value: 1.2},
		{TaskID: "rollback-task", Key: "lr", Step: 1, TimestampMillis: now, Value: math.NaN()},
	}

	if err := store.Flush(ctx, points); err == nil {
		t.Fatal("Flush() error = nil, want transaction failure")
	}
	assertTableCount(t, pool, "metric_points", 0)
	assertTableCount(t, pool, "metric_events", 0)
	assertTableCount(t, pool, "metric_outbox", 0)
	assertTableCount(t, pool, "task_event_counters", 0)
}

func TestMetricPointStoreSerializesConcurrentTaskSequences(t *testing.T) {
	pool, store := newMetricStoreIntegrationDatabase(t)
	ctx := context.Background()
	started := make(chan struct{})
	var wait sync.WaitGroup
	errorsCh := make(chan error, 2)
	for index := 0; index < 2; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-started
			errorsCh <- store.Flush(ctx, []domain.MetricPoint{{
				TaskID:          "concurrent-task",
				Key:             "loss",
				Step:            int64(index + 1),
				TimestampMillis: time.Now().UTC().Add(time.Duration(index) * time.Millisecond).UnixMilli(),
				Value:           float64(index + 1),
			}})
		}()
	}
	close(started)
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent Flush() error = %v", err)
		}
	}

	rows, err := pool.Query(ctx, `SELECT event_seq FROM metric_events WHERE task_id = 'concurrent-task' ORDER BY event_seq`)
	if err != nil {
		t.Fatalf("query concurrent event sequences: %v", err)
	}
	defer rows.Close()
	var sequences []int64
	for rows.Next() {
		var sequence int64
		if err := rows.Scan(&sequence); err != nil {
			t.Fatalf("scan concurrent event sequence: %v", err)
		}
		sequences = append(sequences, sequence)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read concurrent event sequences: %v", err)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	if len(sequences) != 2 || sequences[0] != 1 || sequences[1] != 2 {
		t.Fatalf("event sequences = %v, want [1 2]", sequences)
	}
}

func TestMetricPointStoreKeepsExactlyOnceEffectAfterUnackedRedelivery(t *testing.T) {
	pool, store := newMetricStoreIntegrationDatabase(t)
	ctx := context.Background()
	amqpURL := os.Getenv("AMQP_INTEGRATION_URL")
	if amqpURL == "" {
		amqpURL = "amqp://metrics:metrics@localhost:5672/"
	}
	connection, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("dial integration RabbitMQ: %v", err)
	}
	defer connection.Close()
	control, err := connection.Channel()
	if err != nil {
		t.Fatalf("open integration RabbitMQ control channel: %v", err)
	}
	defer control.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	exchange := "metrics.store.recovery.exchange." + suffix
	queueName := "metrics.store.recovery.queue." + suffix
	routingKey := "metrics.store.recovery." + suffix
	if err := control.ExchangeDeclare(exchange, "direct", false, false, false, false, nil); err != nil {
		t.Fatalf("declare recovery exchange: %v", err)
	}
	queue, err := control.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare recovery queue: %v", err)
	}
	if err := control.QueueBind(queue.Name, routingKey, exchange, false, nil); err != nil {
		t.Fatalf("bind recovery queue: %v", err)
	}
	t.Cleanup(func() {
		_, _ = control.QueueDelete(queue.Name, false, false, false)
		_ = control.ExchangeDelete(exchange, false, false)
	})

	now := time.Now().UTC()
	message := messaging.IngestMessage{
		SchemaVersion: messaging.IngestSchemaVersion,
		MessageID:     "recovery-message",
		CorrelationID: "recovery-message",
		TaskID:        "recovery-task",
		Batch: []messaging.IngestSample{{
			Step:    1,
			TS:      now.UnixMilli(),
			Metrics: map[string]float64{"loss": 1.2, "lr": 0.001},
		}},
	}
	publishing, err := messaging.MarshalIngestPublishing(message, now)
	if err != nil {
		t.Fatalf("marshal recovery message: %v", err)
	}
	if err := control.PublishWithContext(ctx, exchange, routingKey, false, false, publishing); err != nil {
		t.Fatalf("publish recovery message: %v", err)
	}

	firstChannel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open first recovery channel: %v", err)
	}
	firstDeliveries, err := firstChannel.Consume(queue.Name, "recovery-first", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume first recovery delivery: %v", err)
	}
	first := receiveIntegrationDelivery(t, firstDeliveries)
	decoded, err := messaging.DecodeIngestMessage(first.Body)
	if err != nil {
		t.Fatalf("decode first recovery delivery: %v", err)
	}
	if err := store.Flush(ctx, messaging.ExpandMetricPoints(decoded)); err != nil {
		t.Fatalf("commit first recovery delivery: %v", err)
	}
	if err := firstChannel.Close(); err != nil {
		t.Fatalf("close first channel before ack: %v", err)
	}

	secondChannel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open second recovery channel: %v", err)
	}
	defer secondChannel.Close()
	secondDeliveries, err := secondChannel.Consume(queue.Name, "recovery-second", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume redelivered recovery message: %v", err)
	}
	second := receiveIntegrationDelivery(t, secondDeliveries)
	if !second.Redelivered {
		t.Fatal("second delivery Redelivered = false, want broker redelivery")
	}
	decoded, err = messaging.DecodeIngestMessage(second.Body)
	if err != nil {
		t.Fatalf("decode redelivered recovery message: %v", err)
	}
	if err := store.Flush(ctx, messaging.ExpandMetricPoints(decoded)); err != nil {
		t.Fatalf("flush redelivered recovery message: %v", err)
	}
	if err := second.Ack(false); err != nil {
		t.Fatalf("ack redelivered recovery message: %v", err)
	}

	assertTableCount(t, pool, "metric_points", 2)
	assertTableCount(t, pool, "metric_events", 1)
	assertTableCount(t, pool, "metric_outbox", 1)
	assertCounter(t, pool, "recovery-task", 1)
}

func newMetricStoreIntegrationDatabase(t *testing.T) (*pgxpool.Pool, *MetricPointStore) {
	t.Helper()
	ctx := context.Background()
	url := os.Getenv("DATABASE_INTEGRATION_URL")
	if url == "" {
		url = "postgres://metrics:metrics@localhost:5432/metrics?sslmode=disable"
	}
	control, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("open integration control pool: %v", err)
	}
	if err := control.Ping(ctx); err != nil {
		control.Close()
		t.Fatalf("ping integration database: %v", err)
	}

	schema := fmt.Sprintf("metric_store_test_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := control.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		control.Close()
		t.Fatalf("create integration schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = control.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE")
		control.Close()
	})

	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse integration database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open schema integration pool: %v", err)
	}
	t.Cleanup(pool.Close)

	migrationSQL, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000001_initial_schema.sql"))
	if err != nil {
		t.Fatalf("read initial migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
		t.Fatalf("apply initial migration: %v", err)
	}
	if _, err := pool.Exec(ctx, "SELECT ensure_metric_daily_partitions(current_date, 1, 1)"); err != nil {
		t.Fatalf("create integration partitions: %v", err)
	}

	store, err := NewMetricPointStore(pool)
	if err != nil {
		t.Fatalf("NewMetricPointStore() error = %v", err)
	}
	return pool, store
}

func assertTableCount(t *testing.T, pool *pgxpool.Pool, table string, want int) {
	t.Helper()
	query := "SELECT count(*) FROM " + pgx.Identifier{table}.Sanitize()
	var got int
	if err := pool.QueryRow(context.Background(), query).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", table, got, want)
	}
}

func assertCounter(t *testing.T, pool *pgxpool.Pool, taskID string, want int64) {
	t.Helper()
	var got int64
	if err := pool.QueryRow(context.Background(), "SELECT last_event_seq FROM task_event_counters WHERE task_id = $1", taskID).Scan(&got); err != nil {
		t.Fatalf("read task counter %q: %v", taskID, err)
	}
	if got != want {
		t.Fatalf("task counter %q = %d, want %d", taskID, got, want)
	}
}

func receiveIntegrationDelivery(t *testing.T, deliveries <-chan amqp.Delivery) amqp.Delivery {
	t.Helper()
	select {
	case delivery := <-deliveries:
		return delivery
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for RabbitMQ delivery")
		return amqp.Delivery{}
	}
}
