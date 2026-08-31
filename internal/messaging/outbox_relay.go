package messaging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
)

type OutboxRepository interface {
	Claim(context.Context, int, time.Duration) ([]domain.OutboxEvent, string, error)
	MarkPublished(context.Context, domain.OutboxEvent) error
	MarkFailed(context.Context, domain.OutboxEvent, time.Duration) error
	ReleaseClaim(context.Context, domain.OutboxEvent) error
	TryAcquireLeader(context.Context) (func(), bool, error)
}

type MetricEventPublisher interface {
	Publish(context.Context, domain.OutboxEvent) error
	Close() error
}

type OutboxRelayConfig struct {
	BatchSize      int
	PollInterval   time.Duration
	LeaseDuration  time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type OutboxRelay struct {
	config     OutboxRelayConfig
	repository OutboxRepository
	publisher  MetricEventPublisher
	logger     *slog.Logger
}

func NewOutboxRelay(config OutboxRelayConfig, repository OutboxRepository, publisher MetricEventPublisher, logger *slog.Logger) (*OutboxRelay, error) {
	if repository == nil {
		return nil, errors.New("outbox repository is required")
	}
	if publisher == nil {
		return nil, errors.New("metric event publisher is required")
	}
	if config.BatchSize == 0 {
		config.BatchSize = 100
	}
	if config.PollInterval == 0 {
		config.PollInterval = 100 * time.Millisecond
	}
	if config.LeaseDuration == 0 {
		config.LeaseDuration = 30 * time.Second
	}
	if config.InitialBackoff == 0 {
		config.InitialBackoff = 100 * time.Millisecond
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = 30 * time.Second
	}
	if config.BatchSize < 1 || config.PollInterval <= 0 || config.LeaseDuration <= 0 || config.InitialBackoff <= 0 || config.MaxBackoff < config.InitialBackoff {
		return nil, errors.New("invalid outbox relay configuration")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &OutboxRelay{config: config, repository: repository, publisher: publisher, logger: logger}, nil
}

func (r *OutboxRelay) Run(ctx context.Context) error {
	for ctx.Err() == nil {

		// 通过 PostgreSQL advisory lock 选出 Outbox relay leader。
		// 同一数据库中，同一时刻只有一个 worker 实例负责发布 Outbox 事件。
		release, acquired, err := r.repository.TryAcquireLeader(ctx)
		if err != nil {
			r.logger.Warn("outbox relay leader election failed", slog.Any("error", err))
			//失败则退避等待
			if !r.wait(ctx, r.config.PollInterval) {
				return nil
			}
			continue
		}
		if !acquired { //获取锁失败
			if !r.wait(ctx, r.config.PollInterval) {
				return nil
			}
			continue
		}

		err = r.runLeader(ctx)
		release()
		if err != nil && ctx.Err() == nil {
			r.logger.Warn("outbox relay leader loop stopped", slog.Any("error", err))
			if !r.wait(ctx, r.config.PollInterval) {
				return nil
			}
		}
	}
	return nil
}

func (r *OutboxRelay) runLeader(ctx context.Context) error {
	backoff := r.config.InitialBackoff
	for ctx.Err() == nil {
		events, _, err := r.repository.Claim(ctx, r.config.BatchSize, r.config.LeaseDuration)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			if !r.wait(ctx, r.config.PollInterval) {
				return nil
			}
			continue
		}
		for index, event := range events {
			started := time.Now()
			if err := r.publisher.Publish(ctx, event); err != nil {
				markErr := r.repository.MarkFailed(ctx, event, backoff)
				releaseErr := r.releaseClaims(ctx, events[index+1:])
				if markErr != nil || releaseErr != nil {
					return errors.Join(err, markErr, releaseErr)
				}
				r.logger.Warn("metric event publish failed",
					slog.Int64("outbox_id", event.ID), slog.String("task_id", event.TaskID), slog.Int64("event_seq", event.EventSeq),
					slog.Duration("backoff", backoff), slog.Int64("duration_ms", time.Since(started).Milliseconds()), slog.Any("error", err))
				if !r.wait(ctx, backoff) {
					return nil
				}
				backoff = minDuration(backoff*2, r.config.MaxBackoff)
				return nil
			}
			if err := r.repository.MarkPublished(ctx, event); err != nil {
				return errors.Join(err, r.releaseClaims(ctx, events[index+1:]))
			}
			backoff = r.config.InitialBackoff
			r.logger.Debug("metric event published", slog.Int64("outbox_id", event.ID), slog.String("task_id", event.TaskID), slog.Int64("event_seq", event.EventSeq), slog.Int64("duration_ms", time.Since(started).Milliseconds()))
		}
	}
	return nil
}

func (r *OutboxRelay) releaseClaims(ctx context.Context, events []domain.OutboxEvent) error {
	var releaseErr error
	for _, event := range events {
		if err := r.repository.ReleaseClaim(ctx, event); err != nil {
			releaseErr = errors.Join(releaseErr, err)
		}
	}
	return releaseErr
}

func (r *OutboxRelay) wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}
