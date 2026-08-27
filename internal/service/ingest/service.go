// Package ingest 提供指标接入用例编排。
package ingest

import (
	"context"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
)

// Publisher 持久化提交已经通过校验的指标批次。
type Publisher interface {
	PublishMetricBatch(context.Context, domain.MetricBatch) error
}

// Service 承载指标接入用例。
type Service struct {
	publisher Publisher
}

// NewService 创建指标接入服务。
func NewService(publisher Publisher) Service {
	if publisher == nil {
		publisher = discardPublisher{}
	}

	return Service{publisher: publisher}
}

// IngestMetricBatch 提交已解析成领域批次的指标数据。
func (s Service) IngestMetricBatch(ctx context.Context, batch domain.MetricBatch) error {
	if s.publisher == nil {
		return nil
	}

	return s.publisher.PublishMetricBatch(ctx, batch)
}

type discardPublisher struct{}

func (discardPublisher) PublishMetricBatch(context.Context, domain.MetricBatch) error {
	return nil
}
