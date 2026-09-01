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
	"github.com/ougi777/metrics-pipeline-go/internal/service/history"
	"github.com/ougi777/metrics-pipeline-go/internal/service/summary"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestMetricPointStoreQueriesAndDownsamplesHistory(t *testing.T) {
	pool, store := newMetricStoreIntegrationDatabase(t)
	now := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	for step := int32(0); step < 20; step++ {
		if _, err := pool.Exec(context.Background(), `INSERT INTO metric_points (task_id, key, step, ts, value) VALUES ($1, 'loss', $2, $3, $4)`, "history-task", step, now.Add(time.Duration(step)*time.Second), float64(step)); err != nil {
			t.Fatalf("insert loss point: %v", err)
		}
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO metric_points (task_id, key, step, ts, value) VALUES ($1, 'lr', 1, $2, 0.5)`, "history-task", now); err != nil {
		t.Fatalf("insert lr point: %v", err)
	}

	result, err := store.QueryHistory(context.Background(), history.Query{TaskID: "history-task", MaxPoints: 5})
	if err != nil {
		t.Fatalf("QueryHistory() error = %v", err)
	}
	if !result.Exists || !result.Downsampled || result.BucketMS != 3_801 {
		t.Fatalf("metadata = %#v", result)
	}
	if len(result.Points) != 6 {
		t.Fatalf("point count = %d, want 6", len(result.Points))
	}
	if result.Points[0].Key != "loss" || result.Points[0].Min != 0 || result.Points[0].Max != 3 || result.Points[0].V != 1.5 {
		t.Fatalf("first sampled point = %#v", result.Points[0])
	}

	filtered, err := store.QueryHistory(context.Background(), history.Query{TaskID: "history-task", Keys: []string{"lr"}, From: ptrInt64(now.Add(-time.Second).UnixMilli()), To: ptrInt64(now.Add(time.Second).UnixMilli()), MaxPoints: 5})
	if err != nil {
		t.Fatalf("filtered QueryHistory() error = %v", err)
	}
	if len(filtered.Points) != 1 || filtered.Points[0].Key != "lr" || filtered.Downsampled {
		t.Fatalf("filtered result = %#v", filtered)
	}
}

func TestMetricPointStoreDownsampleUsesSmallestStepAtFirstTimestamp(t *testing.T) {
	pool, store := newMetricStoreIntegrationDatabase(t)
	now := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	for _, point := range []struct {
		step  int
		ts    time.Time
		value float64
	}{
		{step: 9, ts: now, value: 9},
		{step: 3, ts: now, value: 3},
		{step: 4, ts: now.Add(time.Second), value: 4},
	} {
		if _, err := pool.Exec(
			context.Background(),
			`INSERT INTO metric_points (task_id, key, step, ts, value) VALUES ('history-tie-task', 'loss', $1, $2, $3)`,
			point.step,
			point.ts,
			point.value,
		); err != nil {
			t.Fatalf("insert history point: %v", err)
		}
	}

	result, err := store.QueryHistory(context.Background(), history.Query{TaskID: "history-tie-task", MaxPoints: 1})
	if err != nil {
		t.Fatalf("QueryHistory() error = %v", err)
	}
	if len(result.Points) != 1 {
		t.Fatalf("point count = %d, want 1", len(result.Points))
	}
	if point := result.Points[0]; point.Step != 3 || point.TS != now.UnixMilli() {
		t.Fatalf("sampled point = %#v, want earliest ts with step 3", point)
	}
}

func TestMetricPointStoreQueriesTaskSummary(t *testing.T) {
	_, store := newMetricStoreIntegrationDatabase(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	points := []domain.MetricPoint{
		{TaskID: "summary-task", Key: "loss", Step: 1, TimestampMillis: now.Add(-3 * time.Second).UnixMilli(), Value: 3},
		{TaskID: "summary-task", Key: "loss", Step: 2, TimestampMillis: now.Add(-2 * time.Second).UnixMilli(), Value: 2},
		{TaskID: "summary-task", Key: "loss", Step: 2, TimestampMillis: now.Add(-time.Second).UnixMilli(), Value: 1},
		{TaskID: "summary-task", Key: "lr", Step: 4, TimestampMillis: now.UnixMilli(), Value: 0.5},
	}
	if err := store.Flush(context.Background(), points); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	started := time.Now()
	result, err := store.QuerySummary(context.Background(), summary.Query{TaskID: "summary-task"})
	t.Logf("summary query duration: %s", time.Since(started))
	if err != nil {
		t.Fatalf("QuerySummary() error = %v", err)
	}
	if !result.Exists || result.LastStep != 4 || result.UpdatedAt != now.UnixMilli() {
		t.Fatalf("task metadata = %#v", result)
	}
	loss := result.Metrics["loss"]
	if loss.Last != 1 || loss.Min != 1 || loss.Max != 3 || loss.Avg != 2 {
		t.Fatalf("loss summary = %#v", loss)
	}
	lr := result.Metrics["lr"]
	if lr.Last != 0.5 || lr.Min != 0.5 || lr.Max != 0.5 || lr.Avg != 0.5 {
		t.Fatalf("lr summary = %#v", lr)
	}

	missing, err := store.QuerySummary(context.Background(), summary.Query{TaskID: "missing-summary-task"})
	if err != nil {
		t.Fatalf("missing QuerySummary() error = %v", err)
	}
	if missing.Exists || len(missing.Metrics) != 0 {
		t.Fatalf("missing summary = %#v", missing)
	}
}

func TestOutboxStoreClaimsAndCompletesEvent(t *testing.T) {
	pool, _ := newMetricStoreIntegrationDatabase(t)
	store, err := NewOutboxStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO metric_outbox (task_id, event_seq, payload) VALUES ('relay-task', 1, '{"points":[]}')`); err != nil {
		t.Fatalf("insert outbox event: %v", err)
	}
	events, token, err := store.Claim(ctx, 100, time.Second)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if len(events) != 1 || token == "" || events[0].ClaimToken != token || events[0].TaskID != "relay-task" || events[0].EventSeq != 1 {
		t.Fatalf("claimed events = %#v, token = %q", events, token)
	}
	if err := store.MarkFailed(ctx, events[0], time.Second); err != nil {
		t.Fatalf("MarkFailed() error = %v", err)
	}
	var attempt int
	if err := pool.QueryRow(ctx, "SELECT attempt_count FROM metric_outbox WHERE task_id = 'relay-task'").Scan(&attempt); err != nil {
		t.Fatal(err)
	}
	if attempt != 1 {
		t.Fatalf("attempt_count = %d, want 1", attempt)
	}
	events, _, err = store.Claim(ctx, 1, time.Second)
	if err != nil {
		t.Fatalf("second Claim() error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("claimed event before retry time = %#v", events)
	}
	time.Sleep(1100 * time.Millisecond)
	events, _, err = store.Claim(ctx, 1, time.Second)
	if err != nil || len(events) != 1 {
		t.Fatalf("retry Claim() = events:%#v err:%v", events, err)
	}
	if err := store.MarkPublished(ctx, events[0]); err != nil {
		t.Fatalf("MarkPublished() error = %v", err)
	}
	var published bool
	if err := pool.QueryRow(ctx, "SELECT published_at IS NOT NULL FROM metric_outbox WHERE task_id = 'relay-task'").Scan(&published); err != nil {
		t.Fatal(err)
	}
	if !published {
		t.Fatal("published_at was not set")
	}
}

func TestOutboxStoreReleasesClaim(t *testing.T) {
	pool, _ := newMetricStoreIntegrationDatabase(t)
	store, err := NewOutboxStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO metric_outbox (task_id, event_seq, payload) VALUES ('release-task', 1, '{"points":[]}')`); err != nil {
		t.Fatalf("insert outbox event: %v", err)
	}
	events, _, err := store.Claim(ctx, 1, time.Hour)
	if err != nil || len(events) != 1 {
		t.Fatalf("Claim() = events:%#v err:%v", events, err)
	}
	if err := store.ReleaseClaim(ctx, events[0]); err != nil {
		t.Fatalf("ReleaseClaim() error = %v", err)
	}
	events, _, err = store.Claim(ctx, 1, time.Hour)
	if err != nil || len(events) != 1 {
		t.Fatalf("Claim() after release = events:%#v err:%v", events, err)
	}
}

func TestOutboxStoreClaimsOnlyTaskHeads(t *testing.T) {
	pool, _ := newMetricStoreIntegrationDatabase(t)
	store, err := NewOutboxStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, row := range []struct {
		task string
		seq  int
	}{
		{task: "ordered-a", seq: 1}, {task: "ordered-a", seq: 2}, {task: "ordered-b", seq: 1},
	} {
		if _, err := pool.Exec(ctx, `INSERT INTO metric_outbox (task_id, event_seq, payload) VALUES ($1, $2, '{"points":[]}')`, row.task, row.seq); err != nil {
			t.Fatalf("insert outbox event: %v", err)
		}
	}
	claimed, _, err := store.Claim(ctx, 10, time.Hour)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("first Claim() = events:%#v err:%v, want two task heads", claimed, err)
	}
	byTask := make(map[string]domain.OutboxEvent, len(claimed))
	for _, event := range claimed {
		byTask[event.TaskID] = event
		if event.EventSeq != 1 {
			t.Fatalf("claimed non-head event = %#v", event)
		}
	}
	if err := store.MarkFailed(ctx, byTask["ordered-a"], time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPublished(ctx, byTask["ordered-b"]); err != nil {
		t.Fatal(err)
	}
	claimed, _, err = store.Claim(ctx, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("Claim() exposed blocked successor = %#v", claimed)
	}
	if _, err := pool.Exec(ctx, `UPDATE metric_outbox SET next_attempt_at = clock_timestamp() WHERE task_id = 'ordered-a' AND event_seq = 1`); err != nil {
		t.Fatal(err)
	}
	retry, _, err := store.Claim(ctx, 10, time.Hour)
	if err != nil || len(retry) != 1 || retry[0].TaskID != "ordered-a" || retry[0].EventSeq != 1 {
		t.Fatalf("Claim() failed head retry = events:%#v err:%v", retry, err)
	}
	if err := store.MarkPublished(ctx, retry[0]); err != nil {
		t.Fatal(err)
	}
	claimed, _, err = store.Claim(ctx, 10, time.Hour)
	if err != nil || len(claimed) != 1 || claimed[0].TaskID != "ordered-a" || claimed[0].EventSeq != 2 {
		t.Fatalf("Claim() successor = events:%#v err:%v", claimed, err)
	}
}

func ptrInt64(value int64) *int64 { return &value }

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

func TestMaintainRetentionDropsWholePartitionsAndCleansBoundaryData(t *testing.T) {
	pool, _ := newMetricStoreIntegrationDatabase(t)
	ctx := context.Background()
	cutoff := time.Now().UTC().Truncate(time.Second).Add(-48 * time.Hour)
	if _, err := pool.Exec(ctx, "SELECT ensure_metric_daily_partitions($1::date, 2, 1)", cutoff); err != nil {
		t.Fatalf("create retention test partitions: %v", err)
	}

	old := cutoff.Add(-24 * time.Hour)
	boundaryExpired := cutoff.Add(-time.Hour)
	retained := cutoff
	insertRetentionData(t, pool, old, "old", true)
	insertRetentionData(t, pool, boundaryExpired, "boundary-expired", true)
	insertRetentionData(t, pool, retained, "retained", true)
	insertRetentionData(t, pool, old.Add(time.Minute), "pending-outbox", false)

	first, err := MaintainRetention(ctx, pool, cutoff, 1)
	if err != nil {
		t.Fatalf("first MaintainRetention() error = %v", err)
	}
	if first.PointPartitionsDropped < 1 || first.EventPartitionsDropped < 1 {
		t.Fatalf("first MaintainRetention() result = %#v, want dropped old partitions", first)
	}
	if first.MetricPointsDeleted != 1 || first.MetricEventsDeleted != 1 || first.MetricOutboxDeleted != 1 {
		t.Fatalf("first MaintainRetention() result = %#v, want one boundary deletion per table", first)
	}

	second, err := MaintainRetention(ctx, pool, cutoff, 1)
	if err != nil {
		t.Fatalf("second MaintainRetention() error = %v", err)
	}
	if second.MetricOutboxDeleted != 1 {
		t.Fatalf("second MaintainRetention() result = %#v, want remaining published Outbox deletion", second)
	}
	third, err := MaintainRetention(ctx, pool, cutoff, 1)
	if err != nil {
		t.Fatalf("third MaintainRetention() error = %v", err)
	}
	if third != (RetentionResult{}) {
		t.Fatalf("third MaintainRetention() result = %#v, want no-op", third)
	}
	assertTableCount(t, pool, "metric_points", 1)
	assertTableCount(t, pool, "metric_events", 1)
	assertTableCount(t, pool, "metric_outbox", 2)

	var counter int64
	if err := pool.QueryRow(ctx, "SELECT last_event_seq FROM task_event_counters WHERE task_id = 'retained'").Scan(&counter); err != nil {
		t.Fatalf("read retained task event counter: %v", err)
	}
	if counter != 1 {
		t.Fatalf("retained task event counter = %d, want 1", counter)
	}
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
	retentionMigrationSQL, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000002_retention_maintenance.sql"))
	if err != nil {
		t.Fatalf("read retention migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(retentionMigrationSQL)); err != nil {
		t.Fatalf("apply retention migration: %v", err)
	}
	queryIndexMigrationSQL, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000003_metric_query_covering_index.sql"))
	if err != nil {
		t.Fatalf("read query index migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(queryIndexMigrationSQL)); err != nil {
		t.Fatalf("apply query index migration: %v", err)
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

func insertRetentionData(t *testing.T, pool *pgxpool.Pool, createdAt time.Time, taskID string, published bool) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
INSERT INTO metric_points (task_id, key, step, ts, value)
VALUES ($1, 'loss', 1, $2, 1.2)
`, taskID, createdAt); err != nil {
		t.Fatalf("insert metric point %q: %v", taskID, err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO task_event_counters (task_id, last_event_seq)
VALUES ($1, 1)
`, taskID); err != nil {
		t.Fatalf("insert event counter %q: %v", taskID, err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO metric_events (created_at, task_id, event_seq, payload)
VALUES ($1, $2, 1, '{"points":[]}')
`, createdAt, taskID); err != nil {
		t.Fatalf("insert metric event %q: %v", taskID, err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO metric_outbox (task_id, event_seq, payload, created_at, published_at)
VALUES ($1, 1, '{"points":[]}', $2::timestamptz, CASE WHEN $3::boolean THEN $2::timestamptz ELSE NULL END)
`, taskID, createdAt, published); err != nil {
		t.Fatalf("insert metric outbox %q: %v", taskID, err)
	}
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
