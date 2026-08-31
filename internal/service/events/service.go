package events

import (
	"context"
	"errors"

	"github.com/ougi777/metrics-pipeline-go/internal/sse"
)

type Repository interface {
	QueryEvents(context.Context, string, int64) ([]sse.Event, error)
	EventBounds(context.Context, string) (int64, int64, error)
}

type Service struct{ repository Repository }

func NewService(repository Repository) Service { return Service{repository: repository} }

func (s Service) Query(ctx context.Context, taskID string, after int64) ([]sse.Event, error) {
	if s.repository == nil {
		return nil, errors.New("event repository is required")
	}
	return s.repository.QueryEvents(ctx, taskID, after)
}

func (s Service) Bounds(ctx context.Context, taskID string) (int64, int64, error) {
	if s.repository == nil {
		return 0, 0, errors.New("event repository is required")
	}
	return s.repository.EventBounds(ctx, taskID)
}
