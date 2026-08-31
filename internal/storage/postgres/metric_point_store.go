package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	"github.com/ougi777/metrics-pipeline-go/internal/service/history"
	"github.com/ougi777/metrics-pipeline-go/internal/service/summary"
	"github.com/ougi777/metrics-pipeline-go/internal/sse"
)

// MetricPointStore 使用一个 PostgreSQL 事务持久化一次 worker flush。
type MetricPointStore struct {
	pool *pgxpool.Pool
}

func (s *MetricPointStore) QueryEvents(ctx context.Context, taskID string, after int64) ([]sse.Event, error) {
	rows, err := s.pool.Query(ctx, queryMetricEventsSQL, taskID, after)
	if err != nil {
		return nil, fmt.Errorf("query metric events: %w", err)
	}
	defer rows.Close()
	events := make([]sse.Event, 0)
	for rows.Next() {
		var event sse.Event
		if err := rows.Scan(&event.EventSeq, &event.Payload); err != nil {
			return nil, fmt.Errorf("scan metric event: %w", err)
		}
		event.TaskID = taskID
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read metric events: %w", err)
	}
	return events, nil
}

func (s *MetricPointStore) EventBounds(ctx context.Context, taskID string) (int64, int64, error) {
	var oldest, latest int64
	if err := s.pool.QueryRow(ctx, queryMetricEventBoundsSQL, taskID).Scan(&oldest, &latest); err != nil {
		return 0, 0, fmt.Errorf("query metric event bounds: %w", err)
	}
	return oldest, latest, nil
}

// QuerySummary aggregates the retained metric points for one task in PostgreSQL.
func (s *MetricPointStore) QuerySummary(ctx context.Context, query summary.Query) (summary.Result, error) {
	cutoff := time.Now().UTC().Add(-168 * time.Hour)
	rows, err := s.pool.Query(ctx, queryMetricSummarySQL, query.TaskID, cutoff)
	if err != nil {
		return summary.Result{}, fmt.Errorf("query metric summary: %w", err)
	}
	defer rows.Close()

	result := summary.Result{Metrics: make(map[string]summary.Metric)}
	for rows.Next() {
		var exists bool
		var lastStep *int32
		var updatedAt *time.Time
		var key *string
		var last, min, max, avg *float64
		if err := rows.Scan(&exists, &lastStep, &updatedAt, &key, &last, &min, &max, &avg); err != nil {
			return summary.Result{}, fmt.Errorf("scan metric summary: %w", err)
		}
		result.Exists = exists
		if lastStep != nil {
			result.LastStep = *lastStep
		}
		if updatedAt != nil {
			result.UpdatedAt = updatedAt.UnixMilli()
		}
		if key != nil {
			result.Metrics[*key] = summary.Metric{Last: *last, Min: *min, Max: *max, Avg: *avg}
		}
	}
	if err := rows.Err(); err != nil {
		return summary.Result{}, fmt.Errorf("read metric summary: %w", err)
	}
	return result, nil
}

// QueryHistory applies all filters and performs bounded aggregation in PostgreSQL.
func (s *MetricPointStore) QueryHistory(ctx context.Context, query history.Query) (history.Result, error) {
	if query.MaxPoints < 1 || query.MaxPoints > 5000 {
		return history.Result{}, fmt.Errorf("max_points must be between 1 and 5000")
	}
	cutoff := time.Now().UTC().Add(-168 * time.Hour)
	args := []any{query.TaskID, cutoff, nullableStrings(query.Keys), nullableMillis(query.From), nullableMillis(query.To), nullableStep(query.StepFrom), nullableStep(query.StepTo), query.MaxPoints}
	rows, err := s.pool.Query(ctx, queryMetricHistorySQL, args...)
	if err != nil {
		return history.Result{}, fmt.Errorf("query metric history: %w", err)
	}
	defer rows.Close()
	result := history.Result{}
	for rows.Next() {
		var exists bool
		var bucket *int64
		var key *string
		var step *int32
		var ts *time.Time
		var v, min, max *float64
		if err := rows.Scan(&exists, &bucket, &key, &step, &ts, &v, &min, &max); err != nil {
			return history.Result{}, fmt.Errorf("scan metric history: %w", err)
		}
		result.Exists = exists
		if bucket != nil {
			result.BucketMS = *bucket
		}
		if key != nil {
			result.Points = append(result.Points, history.Point{Key: *key, Step: *step, TS: ts.UnixMilli(), V: *v, Min: *min, Max: *max})
		}
	}
	if err := rows.Err(); err != nil {
		return history.Result{}, fmt.Errorf("read metric history: %w", err)
	}
	result.Downsampled = result.BucketMS > 0
	return result, nil
}

func nullableStrings(value []string) any {
	if len(value) == 0 {
		return nil
	}
	return value
}
func nullableMillis(value *int64) any {
	if value == nil {
		return nil
	}
	return time.UnixMilli(*value).UTC()
}
func nullableStep(value *int64) any {
	if value == nil {
		return nil
	}
	return int32(*value)
}

type generatedEvent struct {
	TaskID   string
	EventSeq int64
	Payload  json.RawMessage
}

type metricPointColumns struct {
	taskIDs    []string
	keys       []string
	steps      []int32
	timestamps []time.Time
	values     []float64
}

func NewMetricPointStore(pool *pgxpool.Pool) (*MetricPointStore, error) {
	if pool == nil {
		return nil, errors.New("postgres pool is required")
	}

	return &MetricPointStore{pool: pool}, nil
}

// Flush 原子写入指标点、任务事件和 Outbox。返回 nil 表示事务已经提交。
func (s *MetricPointStore) Flush(ctx context.Context, points []domain.MetricPoint) error {
	if len(points) == 0 {
		return nil
	}

	columns, taskIDs, err := encodeMetricPoints(points)
	if err != nil {
		return err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin metric flush transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()
	if err := ensureMetricPointPartitions(ctx, tx, columns.timestamps, time.Now().UTC()); err != nil {
		return err
	}

	var batch pgx.Batch
	batch.Queue(seedTaskEventCountersSQL, taskIDs)
	batch.Queue(
		persistMetricFlushSQL,
		columns.taskIDs,
		columns.keys,
		columns.steps,
		columns.timestamps,
		columns.values,
	)

	results := tx.SendBatch(ctx, &batch)
	if _, err := results.Exec(); err != nil {
		return closeBatchWithError(results, fmt.Errorf("seed task event counters: %w", err))
	}

	rows, err := results.Query()
	if err != nil {
		return closeBatchWithError(results, fmt.Errorf("persist metric flush: %w", err))
	}
	for rows.Next() {
		var event generatedEvent
		if err := rows.Scan(&event.TaskID, &event.EventSeq, &event.Payload); err != nil {
			rows.Close()
			return closeBatchWithError(results, fmt.Errorf("scan generated metric event: %w", err))
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return closeBatchWithError(results, fmt.Errorf("read generated metric events: %w", err))
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close metric flush batch: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit metric flush transaction: %w", err)
	}

	return nil
}

func ensureMetricPointPartitions(
	ctx context.Context,
	tx pgx.Tx,
	timestamps []time.Time,
	referenceTime time.Time,
) error {
	referenceDay := referenceTime.UTC().Truncate(24 * time.Hour)
	windowStart := referenceDay.AddDate(0, 0, -partitionPastDays)
	windowEnd := referenceDay.AddDate(0, 0, partitionFutureDays+1)
	dates := make(map[string]struct{})
	for _, timestamp := range timestamps {
		day := timestamp.UTC().Truncate(24 * time.Hour)
		if !day.Before(windowStart) && day.Before(windowEnd) {
			continue
		}
		dates[day.Format(time.DateOnly)] = struct{}{}
	}

	sortedDates := make([]string, 0, len(dates))
	for date := range dates {
		sortedDates = append(sortedDates, date)
	}
	sort.Strings(sortedDates)
	for _, date := range sortedDates {
		if _, err := tx.Exec(ctx, ensureMetricPointPartitionSQL, date); err != nil {
			return fmt.Errorf("ensure metric point partition for %s: %w", date, err)
		}
	}

	return nil
}

func encodeMetricPoints(points []domain.MetricPoint) (metricPointColumns, []string, error) {
	columns := metricPointColumns{
		taskIDs:    make([]string, 0, len(points)),
		keys:       make([]string, 0, len(points)),
		steps:      make([]int32, 0, len(points)),
		timestamps: make([]time.Time, 0, len(points)),
		values:     make([]float64, 0, len(points)),
	}
	taskSet := make(map[string]struct{})
	for index, point := range points {
		if point.Step < 0 || point.Step > int64(^uint32(0)>>1) {
			return metricPointColumns{}, nil, fmt.Errorf("metric point %d step is outside int32 range", index)
		}
		columns.taskIDs = append(columns.taskIDs, point.TaskID)
		columns.keys = append(columns.keys, point.Key)
		columns.steps = append(columns.steps, int32(point.Step))
		columns.timestamps = append(columns.timestamps, time.UnixMilli(point.TimestampMillis).UTC())
		columns.values = append(columns.values, point.Value)
		taskSet[point.TaskID] = struct{}{}
	}

	taskIDs := make([]string, 0, len(taskSet))
	for taskID := range taskSet {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Strings(taskIDs)

	return columns, taskIDs, nil
}

func closeBatchWithError(results pgx.BatchResults, operationErr error) error {
	if closeErr := results.Close(); closeErr != nil {
		return errors.Join(operationErr, fmt.Errorf("close metric flush batch: %w", closeErr))
	}

	return operationErr
}
