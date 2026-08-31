// Package history provides the historical metrics query use case.
package history

import (
	"context"
	"errors"
)

type Point struct {
	Key  string
	Step int32
	TS   int64
	V    float64
	Min  float64
	Max  float64
}

type Query struct {
	TaskID    string
	Keys      []string
	From      *int64
	To        *int64
	StepFrom  *int64
	StepTo    *int64
	MaxPoints int
}

type Result struct {
	Exists      bool
	Downsampled bool
	BucketMS    int64
	Points      []Point
}

type Repository interface {
	QueryHistory(context.Context, Query) (Result, error)
}

type Service struct{ repository Repository }

func NewService(repository Repository) Service { return Service{repository: repository} }

func (s Service) Query(ctx context.Context, query Query) (Result, error) {
	if s.repository == nil {
		return Result{}, errors.New("history repository is required")
	}
	return s.repository.QueryHistory(ctx, query)
}
