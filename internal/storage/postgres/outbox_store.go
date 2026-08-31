package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ougi777/metrics-pipeline-go/internal/domain"
)

const outboxRelayAdvisoryLockKey int64 = 0x6d657472696373

// 对outbox消息表操作的方法
type OutboxStore struct{ pool *pgxpool.Pool }

func NewOutboxStore(pool *pgxpool.Pool) (*OutboxStore, error) {
	if pool == nil {
		return nil, errors.New("postgres pool is required")
	}
	return &OutboxStore{pool: pool}, nil
}

func (s *OutboxStore) Claim(ctx context.Context, limit int, lease time.Duration) ([]domain.OutboxEvent, string, error) {
	if limit < 1 || lease <= 0 {
		return nil, "", errors.New("invalid outbox claim parameters")
	}
	token, err := newClaimToken()
	if err != nil {
		return nil, "", fmt.Errorf("generate outbox claim token: %w", err)
	}
	rows, err := s.pool.Query(ctx, claimMetricOutboxSQL, limit, token, lease.String())
	if err != nil {
		return nil, "", fmt.Errorf("claim metric outbox: %w", err)
	}
	defer rows.Close()
	events := make([]domain.OutboxEvent, 0, limit)
	for rows.Next() {
		var event domain.OutboxEvent
		if err := rows.Scan(&event.ID, &event.TaskID, &event.EventSeq, &event.Payload); err != nil {
			return nil, "", fmt.Errorf("scan claimed metric outbox: %w", err)
		}
		event.ClaimToken = token
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("read claimed metric outbox: %w", err)
	}
	return events, token, nil
}

func (s *OutboxStore) MarkPublished(ctx context.Context, event domain.OutboxEvent) error {
	result, err := s.pool.Exec(ctx, markMetricOutboxPublishedSQL, event.ID, event.ClaimToken)
	if err != nil {
		return fmt.Errorf("mark metric outbox published: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("mark metric outbox published: claim lost for id %d", event.ID)
	}
	return nil
}

func (s *OutboxStore) MarkFailed(ctx context.Context, event domain.OutboxEvent, backoff time.Duration) error {
	result, err := s.pool.Exec(ctx, markMetricOutboxFailedSQL, event.ID, event.ClaimToken, backoff.String())
	if err != nil {
		return fmt.Errorf("mark metric outbox failed: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("mark metric outbox failed: claim lost for id %d", event.ID)
	}
	return nil
}

func (s *OutboxStore) ReleaseClaim(ctx context.Context, event domain.OutboxEvent) error {
	result, err := s.pool.Exec(ctx, releaseMetricOutboxClaimSQL, event.ID, event.ClaimToken)
	if err != nil {
		return fmt.Errorf("release metric outbox claim: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("release metric outbox claim: claim lost for id %d", event.ID)
	}
	return nil
}

func (s *OutboxStore) TryAcquireLeader(ctx context.Context) (func(), bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire outbox leader connection: %w", err)
	}
	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1::bigint)", outboxRelayAdvisoryLockKey).Scan(&acquired); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("try acquire outbox leader lock: %w", err)
	}
	if !acquired {
		conn.Release()
		return nil, false, nil
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1::bigint)", outboxRelayAdvisoryLockKey)
			conn.Release()
		})
	}
	return release, true, nil
}

func newClaimToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

var _ interface {
	Claim(context.Context, int, time.Duration) ([]domain.OutboxEvent, string, error)
	MarkPublished(context.Context, domain.OutboxEvent) error
	MarkFailed(context.Context, domain.OutboxEvent, time.Duration) error
	ReleaseClaim(context.Context, domain.OutboxEvent) error
	TryAcquireLeader(context.Context) (func(), bool, error)
} = (*OutboxStore)(nil)
