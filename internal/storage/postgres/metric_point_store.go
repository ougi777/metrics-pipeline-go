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
)

// MetricPointStore 使用一个 PostgreSQL 事务持久化一次 worker flush。
type MetricPointStore struct {
	pool *pgxpool.Pool
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
