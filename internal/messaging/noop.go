package messaging

import (
	"context"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
)

type NoopMetricBatchPublisher struct{}

func (NoopMetricBatchPublisher) PublishMetricBatch(context.Context, domain.MetricBatch) error {
	return nil
}
