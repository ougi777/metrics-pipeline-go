package ingest

import (
	"context"
	"errors"
	"testing"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
)

func TestServiceIngestMetricBatchPublishesBatch(t *testing.T) {
	publisher := &recordingPublisher{}
	service := NewService(publisher)
	batch := domain.MetricBatch{
		TaskID: "ft-20260825-0001",
		Samples: []domain.MetricSample{
			{
				Step:            1,
				TimestampMillis: 1756089600123,
				Metrics:         map[string]float64{"loss": 1.2},
			},
		},
	}

	if err := service.IngestMetricBatch(context.Background(), batch); err != nil {
		t.Fatalf("IngestMetricBatch() error = %v", err)
	}

	if len(publisher.batches) != 1 {
		t.Fatalf("published batches = %d, want 1", len(publisher.batches))
	}
	if publisher.batches[0].TaskID != batch.TaskID {
		t.Fatalf("published TaskID = %q, want %q", publisher.batches[0].TaskID, batch.TaskID)
	}
}

func TestServiceIngestMetricBatchReturnsPublisherError(t *testing.T) {
	expectedErr := errors.New("publisher failed")
	service := NewService(&recordingPublisher{err: expectedErr})

	err := service.IngestMetricBatch(context.Background(), domain.MetricBatch{})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("IngestMetricBatch() error = %v, want %v", err, expectedErr)
	}
}

type recordingPublisher struct {
	err     error
	batches []domain.MetricBatch
}

func (p *recordingPublisher) PublishMetricBatch(_ context.Context, batch domain.MetricBatch) error {
	p.batches = append(p.batches, batch)
	return p.err
}
