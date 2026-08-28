package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	partitionPastDays        = 8
	partitionFutureDays      = 2
	retentionWindow          = 168 * time.Hour
	partitionRefreshInterval = time.Hour
	retentionDeleteBatchSize = 10000
)

type partitionExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type retentionExecutor interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// RetentionResult 记录一次保留维护批次的实际变更。
type RetentionResult struct {
	PointPartitionsDropped int
	EventPartitionsDropped int
	MetricPointsDeleted    int
	MetricEventsDeleted    int
	MetricOutboxDeleted    int
}

// EnsureWorkerDailyPartitions 创建 worker 当前写入窗口所需的 UTC 日分区。
func EnsureWorkerDailyPartitions(ctx context.Context, executor partitionExecutor) error {
	return ensureDailyPartitions(
		ctx,
		executor,
		time.Now().UTC(),
		partitionPastDays,
		partitionFutureDays,
	)
}

// RunDailyPartitionMaintenance 持续维护 worker 写入窗口所需的 UTC 日分区。
func RunDailyPartitionMaintenance(
	ctx context.Context,
	executor interface {
		partitionExecutor
		retentionExecutor
	},
	logger *slog.Logger,
) error {
	return RunPartitionMaintenance(
		ctx,
		executor,
		logger,
		retentionWindow,
		partitionRefreshInterval,
		retentionDeleteBatchSize,
	)
}

// RunPartitionMaintenance 按固定周期预建分区并清理超过保留窗口的数据。
func RunPartitionMaintenance(
	ctx context.Context,
	executor interface {
		partitionExecutor
		retentionExecutor
	},
	logger *slog.Logger,
	window time.Duration,
	interval time.Duration,
	batchSize int,
) error {
	if logger == nil {
		logger = slog.Default()
	}
	if window <= 0 {
		return fmt.Errorf("retention window must be greater than zero")
	}
	if interval <= 0 {
		return fmt.Errorf("partition maintenance interval must be greater than zero")
	}
	if batchSize < 1 || batchSize > 100000 {
		return fmt.Errorf("retention batch size must be between 1 and 100000")
	}

	maintain := func() error {
		if err := EnsureWorkerDailyPartitions(ctx, executor); err != nil {
			return err
		}
		logger.Info("postgres daily partitions ensured",
			slog.Int("past_days", partitionPastDays),
			slog.Int("future_days", partitionFutureDays))

		cutoff := time.Now().UTC().Add(-window)
		started := time.Now()
		for {
			result, err := MaintainRetention(ctx, executor, cutoff, batchSize)
			if err != nil {
				return err
			}
			logger.Info("postgres retention maintenance completed",
				slog.Time("cutoff", cutoff),
				slog.Int("point_partitions_dropped", result.PointPartitionsDropped),
				slog.Int("event_partitions_dropped", result.EventPartitionsDropped),
				slog.Int("metric_points_deleted", result.MetricPointsDeleted),
				slog.Int("metric_events_deleted", result.MetricEventsDeleted),
				slog.Int("metric_outbox_deleted", result.MetricOutboxDeleted),
				slog.Int64("duration_ms", time.Since(started).Milliseconds()))
			if result.MetricPointsDeleted < batchSize && result.MetricEventsDeleted < batchSize && result.MetricOutboxDeleted < batchSize {
				return nil
			}
		}
	}

	if err := maintain(); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := maintain(); err != nil {
				return err
			}
		}
	}
}

// MaintainRetention 清理一个批次的过期数据，并删除已完整过期的日分区。
func MaintainRetention(
	ctx context.Context,
	executor retentionExecutor,
	cutoff time.Time,
	batchSize int,
) (RetentionResult, error) {
	if executor == nil {
		return RetentionResult{}, fmt.Errorf("retention executor is required")
	}
	if cutoff.IsZero() {
		return RetentionResult{}, fmt.Errorf("retention cutoff is required")
	}
	if batchSize < 1 || batchSize > 100000 {
		return RetentionResult{}, fmt.Errorf("retention batch size must be between 1 and 100000")
	}

	var result RetentionResult
	err := executor.QueryRow(ctx, maintainMetricRetentionSQL, cutoff.UTC(), batchSize).Scan(
		&result.PointPartitionsDropped,
		&result.EventPartitionsDropped,
		&result.MetricPointsDeleted,
		&result.MetricEventsDeleted,
		&result.MetricOutboxDeleted,
	)
	if err != nil {
		return RetentionResult{}, fmt.Errorf("maintain metric retention: %w", err)
	}
	return result, nil
}

func ensureDailyPartitions(
	ctx context.Context,
	executor partitionExecutor,
	referenceDay time.Time,
	pastDays int,
	futureDays int,
) error {
	if pastDays < 0 || futureDays < 0 || pastDays > 366 || futureDays > 366 {
		return fmt.Errorf("partition day ranges must be between 0 and 366")
	}
	if executor == nil {
		return fmt.Errorf("partition executor is required")
	}

	if _, err := executor.Exec(
		ctx,
		ensureDailyPartitionsSQL,
		referenceDay.UTC().Format(time.DateOnly),
		pastDays,
		futureDays,
	); err != nil {
		return fmt.Errorf("ensure daily partitions: %w", err)
	}

	return nil
}
