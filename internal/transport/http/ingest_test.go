package httptransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	ingestservice "github.com/ougi777/metrics-pipeline-go/internal/service/ingest"
)

func TestIngestMetricsAcceptsValidBatch(t *testing.T) {
	publisher := &recordingMetricBatchPublisher{}
	response := performIngestRequest(t, publisher, `{
		"task_id": "ft-20260825-0001",
		"batch": [
			{
				"step": 120,
				"ts": 1756089600123,
				"metrics": {
					"loss": 1.234,
					"lr": 0.00003,
					"eval_loss": 1.41,
					"gpu_util": 86.5,
					"gpu_mem": 72.3,
					"throughput": 912.8
				}
			},
			{
				"step": 121,
				"ts": 1756089601123,
				"metrics": {
					"loss": 1.198,
					"lr": 0.00003
				}
			}
		]
	}`)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", response.Code, http.StatusOK, response.Body.String())
	}

	var body struct {
		Accepted int    `json:"accepted"`
		TaskID   string `json:"task_id"`
	}
	decodeResponse(t, response, &body)

	if body.Accepted != 2 {
		t.Errorf("accepted = %d, want 2", body.Accepted)
	}
	if body.TaskID != "ft-20260825-0001" {
		t.Errorf("task_id = %q, want ft-20260825-0001", body.TaskID)
	}
	if len(publisher.batches) != 1 {
		t.Fatalf("published batches = %d, want 1", len(publisher.batches))
	}
	published := publisher.batches[0]
	if published.TaskID != "ft-20260825-0001" {
		t.Errorf("published TaskID = %q, want ft-20260825-0001", published.TaskID)
	}
	if len(published.Samples) != 2 {
		t.Fatalf("published sample count = %d, want 2", len(published.Samples))
	}
	if published.Samples[0].Metrics["throughput"] != 912.8 {
		t.Errorf("throughput = %f, want 912.8", published.Samples[0].Metrics["throughput"])
	}
}

func TestIngestMetricsRejectsInvalidBatches(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "empty task id",
			body: `{"task_id":"","batch":[{"step":0,"ts":1756089600123,"metrics":{"loss":1.2}}]}`,
		},
		{
			name: "invalid task id",
			body: `{"task_id":"ft:bad","batch":[{"step":0,"ts":1756089600123,"metrics":{"loss":1.2}}]}`,
		},
		{
			name: "empty batch",
			body: `{"task_id":"ft-20260825-0001","batch":[]}`,
		},
		{
			name: "missing step",
			body: `{"task_id":"ft-20260825-0001","batch":[{"ts":1756089600123,"metrics":{"loss":1.2}}]}`,
		},
		{
			name: "negative step",
			body: `{"task_id":"ft-20260825-0001","batch":[{"step":-1,"ts":1756089600123,"metrics":{"loss":1.2}}]}`,
		},
		{
			name: "step above int32",
			body: `{"task_id":"ft-20260825-0001","batch":[{"step":2147483648,"ts":1756089600123,"metrics":{"loss":1.2}}]}`,
		},
		{
			name: "missing timestamp",
			body: `{"task_id":"ft-20260825-0001","batch":[{"step":0,"metrics":{"loss":1.2}}]}`,
		},
		{
			name: "zero timestamp",
			body: `{"task_id":"ft-20260825-0001","batch":[{"step":0,"ts":0,"metrics":{"loss":1.2}}]}`,
		},
		{
			name: "timestamp above supported range",
			body: `{"task_id":"ft-20260825-0001","batch":[{"step":0,"ts":253402300800000,"metrics":{"loss":1.2}}]}`,
		},
		{
			name: "empty metrics",
			body: `{"task_id":"ft-20260825-0001","batch":[{"step":0,"ts":1756089600123,"metrics":{}}]}`,
		},
		{
			name: "invalid metric key",
			body: `{"task_id":"ft-20260825-0001","batch":[{"step":0,"ts":1756089600123,"metrics":{"loss:train":1.2}}]}`,
		},
		{
			name: "metric key above length",
			body: `{"task_id":"ft-20260825-0001","batch":[{"step":0,"ts":1756089600123,"metrics":{"this_metric_key_is_longer_than_32":1.2}}]}`,
		},
		{
			name: "metric value string",
			body: `{"task_id":"ft-20260825-0001","batch":[{"step":0,"ts":1756089600123,"metrics":{"loss":"1.2"}}]}`,
		},
		{
			name: "unknown top level field",
			body: `{"task_id":"ft-20260825-0001","batch":[{"step":0,"ts":1756089600123,"metrics":{"loss":1.2}}],"extra":true}`,
		},
		{
			name: "unknown sample field",
			body: `{"task_id":"ft-20260825-0001","batch":[{"step":0,"ts":1756089600123,"metrics":{"loss":1.2},"extra":true}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisher := &recordingMetricBatchPublisher{}
			response := performIngestRequest(t, publisher, tt.body)

			assertErrorResponse(t, response, http.StatusBadRequest, ErrorCodeInvalidParams)
			if len(publisher.batches) != 0 {
				t.Fatalf("published batches = %d, want 0", len(publisher.batches))
			}
		})
	}
}

func TestIngestMetricsRejectsOversizedBatch(t *testing.T) {
	publisher := &recordingMetricBatchPublisher{}
	response := performIngestRequest(t, publisher, oversizedBatchBody(t))

	assertErrorResponse(t, response, http.StatusBadRequest, ErrorCodeInvalidParams)
	if len(publisher.batches) != 0 {
		t.Fatalf("published batches = %d, want 0", len(publisher.batches))
	}
}

func TestIngestMetricsRejectsWholeBatch(t *testing.T) {
	publisher := &recordingMetricBatchPublisher{}
	response := performIngestRequest(t, publisher, `{
		"task_id": "ft-20260825-0001",
		"batch": [
			{"step": 1, "ts": 1756089600123, "metrics": {"loss": 1.2}},
			{"step": -1, "ts": 1756089601123, "metrics": {"loss": 1.1}}
		]
	}`)

	assertErrorResponse(t, response, http.StatusBadRequest, ErrorCodeInvalidParams)
	if len(publisher.batches) != 0 {
		t.Fatalf("published batches = %d, want 0", len(publisher.batches))
	}
}

func TestIngestMetricsRejectsWrongContentType(t *testing.T) {
	publisher := &recordingMetricBatchPublisher{}
	router := NewRouter(RouterOptions{IngestService: ingestservice.NewService(publisher)})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "text/plain")

	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	assertErrorResponse(t, response, http.StatusBadRequest, ErrorCodeInvalidParams)
	if len(publisher.batches) != 0 {
		t.Fatalf("published batches = %d, want 0", len(publisher.batches))
	}
}

func TestIngestMetricsReturnsMQUnavailableWhenPublisherFails(t *testing.T) {
	publisher := &recordingMetricBatchPublisher{err: errors.New("publisher unavailable")}
	response := performIngestRequest(t, publisher, `{
		"task_id": "ft-20260825-0001",
		"batch": [
			{"step": 1, "ts": 1756089600123, "metrics": {"loss": 1.2}}
		]
	}`)

	assertErrorResponse(t, response, http.StatusServiceUnavailable, ErrorCodeMQUnavailable)
	if len(publisher.batches) != 1 {
		t.Fatalf("published attempts = %d, want 1", len(publisher.batches))
	}
}

func performIngestRequest(t *testing.T, publisher *recordingMetricBatchPublisher, body string) *httptest.ResponseRecorder {
	t.Helper()

	router := NewRouter(RouterOptions{IngestService: ingestservice.NewService(publisher)})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")

	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	return response
}

func assertErrorResponse(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()

	if response.Code != status {
		t.Fatalf("status = %d, want %d; body: %s", response.Code, status, response.Body.String())
	}

	var body ErrorResponse
	decodeResponse(t, response, &body)
	if body.Error.Code != code {
		t.Fatalf("error code = %q, want %q", body.Error.Code, code)
	}
	if body.Error.Message == "" {
		t.Fatal("error message is empty")
	}
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()

	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response body %q: %v", response.Body.String(), err)
	}
}

func oversizedBatchBody(t *testing.T) string {
	t.Helper()

	batch := make([]map[string]any, maxMetricBatchSize+1)
	for index := range batch {
		batch[index] = map[string]any{
			"step":    index,
			"ts":      int64(1756089600123 + index),
			"metrics": map[string]float64{"loss": 1.2},
		}
	}

	body := map[string]any{
		"task_id": "ft-20260825-0001",
		"batch":   batch,
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	return string(encoded)
}

type recordingMetricBatchPublisher struct {
	err     error
	batches []domain.MetricBatch
}

func (p *recordingMetricBatchPublisher) PublishMetricBatch(_ context.Context, batch domain.MetricBatch) error {
	p.batches = append(p.batches, batch)
	return p.err
}

func TestIngestMetricsRejectsBodyAboveLimit(t *testing.T) {
	publisher := &recordingMetricBatchPublisher{}
	response := performIngestRequest(t, publisher, strings.Repeat(" ", maxIngestBodyBytes)+`{}`)

	assertErrorResponse(t, response, http.StatusBadRequest, ErrorCodeInvalidParams)
	if len(publisher.batches) != 0 {
		t.Fatalf("published batches = %d, want 0", len(publisher.batches))
	}
}

func TestIngestMetricsRejectsTrailingJSON(t *testing.T) {
	publisher := &recordingMetricBatchPublisher{}
	response := performIngestRequest(t, publisher, `{"task_id":"ft-20260825-0001","batch":[{"step":0,"ts":1756089600123,"metrics":{"loss":1.2}}]} {}`)

	assertErrorResponse(t, response, http.StatusBadRequest, ErrorCodeInvalidParams)
	if len(publisher.batches) != 0 {
		t.Fatalf("published batches = %d, want 0", len(publisher.batches))
	}
}

func TestIngestMetricsAcceptsJSONContentTypeWithCharset(t *testing.T) {
	publisher := &recordingMetricBatchPublisher{}
	router := NewRouter(RouterOptions{IngestService: ingestservice.NewService(publisher)})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewBufferString(`{
		"task_id":"ft-20260825-0001",
		"batch":[{"step":0,"ts":1756089600123,"metrics":{"loss":1.2}}]
	}`))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")

	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", response.Code, http.StatusOK, response.Body.String())
	}
	if len(publisher.batches) != 1 {
		t.Fatalf("published batches = %d, want 1", len(publisher.batches))
	}
}
