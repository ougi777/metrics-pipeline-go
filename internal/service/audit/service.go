package audit

import (
	"context"
	"errors"
)

type Query struct{ TaskID string }

type Result struct {
	Exists        bool
	PointCount    int64
	DistinctSteps int64
	FirstStep     *int32
	LastStep      *int32
	Keys          []string
	MissingSteps  []int32
}

type Repository interface {
	QueryAudit(context.Context, Query) (Result, error)
}
type Service struct{ repository Repository }

func NewService(repository Repository) Service { return Service{repository: repository} }
func (s Service) Query(ctx context.Context, query Query) (Result, error) {
	if s.repository == nil {
		return Result{}, errors.New("audit repository is required")
	}
	return s.repository.QueryAudit(ctx, query)
}
