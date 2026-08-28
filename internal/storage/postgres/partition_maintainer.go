package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	partitionPastDays        = 8
	partitionFutureDays      = 2
	partitionRefreshInterval = 12 * time.Hour
)

type partitionExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
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
	executor partitionExecutor,
	logger *slog.Logger,
) error {
	ensure := func() error {
		if err := EnsureWorkerDailyPartitions(ctx, executor); err != nil {
			return err
		}
		logger.Info(
			"postgres daily partitions ensured",
			slog.Int("past_days", partitionPastDays),
			slog.Int("future_days", partitionFutureDays),
		)
		return nil
	}

	if err := ensure(); err != nil {
		return err
	}

	ticker := time.NewTicker(partitionRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := ensure(); err != nil {
				return err
			}
		}
	}
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
