// Package summary provides the task metrics summary use case.
package summary

import (
	"context"
	"errors"
)

type Metric struct {
	Last float64
	Min  float64
	Max  float64
	Avg  float64
}

type Query struct {
	TaskID string
}

type Result struct {
	Exists    bool
	LastStep  int32
	UpdatedAt int64
	Metrics   map[string]Metric
}

type Repository interface {
	QuerySummary(context.Context, Query) (Result, error)
}

type Service struct{ repository Repository }

func NewService(repository Repository) Service {
	return Service{repository: repository}
}

func (s Service) Query(ctx context.Context, query Query) (Result, error) {
	if s.repository == nil {
		return Result{}, errors.New("summary repository is required")
	}
	return s.repository.QuerySummary(ctx, query)
}
