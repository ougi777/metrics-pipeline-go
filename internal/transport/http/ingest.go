package httptransport

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	ingestservice "github.com/ougi777/metrics-pipeline-go/internal/service/ingest"
)

const (
	maxIngestBodyBytes     = 1 << 20
	maxMetricBatchSize     = 500
	maxTaskIDLength        = 64
	maxMetricKeyLength     = 32
	maxStepValue           = math.MaxInt32
	maxTimestampMillis     = 253402300799999
	jsonContentType        = "application/json"
	invalidRequestMessage  = "invalid request body"
	invalidMetricBatchText = "invalid metric batch"
)

var (
	taskIDPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	metricKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

type IngestHandler struct {
	service ingestservice.Service
}

func NewIngestHandler(service ingestservice.Service) IngestHandler {
	return IngestHandler{service: service}
}

func (h IngestHandler) IngestMetrics(c *gin.Context) {
	if !hasJSONContentType(c.GetHeader("Content-Type")) {
		WriteError(c, http.StatusBadRequest, ErrorCodeInvalidParams, "content type must be application/json")
		return
	}

	request, err := decodeIngestMetricsRequest(c)
	if err != nil {
		WriteError(c, http.StatusBadRequest, ErrorCodeInvalidParams, invalidRequestMessage)
		return
	}

	if err := request.Validate(); err != nil {
		WriteError(c, http.StatusBadRequest, ErrorCodeInvalidParams, invalidMetricBatchText)
		return
	}

	batch := request.ToMetricBatch()
	if err := h.service.IngestMetricBatch(c.Request.Context(), batch); err != nil {
		WriteError(c, http.StatusServiceUnavailable, ErrorCodeMQUnavailable, "message queue unavailable")
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"accepted": len(request.Batch),
		"task_id":  request.TaskID,
	})
}

type IngestMetricsRequest struct {
	TaskID string                `json:"task_id"`
	Batch  []MetricSampleRequest `json:"batch"`
}

type MetricSampleRequest struct {
	Step    *int64             `json:"step"`
	TS      *int64             `json:"ts"`
	Metrics map[string]float64 `json:"metrics"`
}

func (r IngestMetricsRequest) Validate() error {
	if r.TaskID == "" {
		return errors.New("task_id is required")
	}
	if len(r.TaskID) > maxTaskIDLength {
		return fmt.Errorf("task_id must be %d characters or fewer", maxTaskIDLength)
	}
	if !taskIDPattern.MatchString(r.TaskID) {
		return errors.New("task_id has invalid format")
	}

	if len(r.Batch) == 0 {
		return errors.New("batch must contain at least 1 sample")
	}
	if len(r.Batch) > maxMetricBatchSize {
		return fmt.Errorf("batch must contain %d samples or fewer", maxMetricBatchSize)
	}

	for index, sample := range r.Batch {
		if err := sample.Validate(index); err != nil {
			return err
		}
	}

	return nil
}

func (r IngestMetricsRequest) ToMetricBatch() domain.MetricBatch {
	samples := make([]domain.MetricSample, 0, len(r.Batch))
	for _, sample := range r.Batch {
		metrics := make(map[string]float64, len(sample.Metrics))
		for key, value := range sample.Metrics {
			metrics[key] = value
		}

		samples = append(samples, domain.MetricSample{
			Step:            *sample.Step,
			TimestampMillis: *sample.TS,
			Metrics:         metrics,
		})
	}

	return domain.MetricBatch{
		TaskID:  r.TaskID,
		Samples: samples,
	}
}

func (s MetricSampleRequest) Validate(index int) error {
	if s.Step == nil {
		return fmt.Errorf("batch[%d].step is required", index)
	}
	if *s.Step < 0 {
		return fmt.Errorf("batch[%d].step must be non-negative", index)
	}
	if *s.Step > maxStepValue {
		return fmt.Errorf("batch[%d].step must be %d or fewer", index, maxStepValue)
	}

	if s.TS == nil {
		return fmt.Errorf("batch[%d].ts is required", index)
	}
	if *s.TS <= 0 {
		return fmt.Errorf("batch[%d].ts must be a positive unix millisecond timestamp", index)
	}
	if *s.TS > maxTimestampMillis {
		return fmt.Errorf("batch[%d].ts is outside supported timestamp range", index)
	}

	if len(s.Metrics) == 0 {
		return fmt.Errorf("batch[%d].metrics must contain at least 1 metric", index)
	}
	for key, value := range s.Metrics {
		if key == "" {
			return fmt.Errorf("batch[%d].metrics contains an empty key", index)
		}
		if len(key) > maxMetricKeyLength {
			return fmt.Errorf("batch[%d].metrics key %q must be %d characters or fewer", index, key, maxMetricKeyLength)
		}
		if !metricKeyPattern.MatchString(key) {
			return fmt.Errorf("batch[%d].metrics key %q has invalid format", index, key)
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("batch[%d].metrics[%q] must be finite", index, key)
		}
	}

	return nil
}

func decodeIngestMetricsRequest(c *gin.Context) (IngestMetricsRequest, error) {
	body := http.MaxBytesReader(c.Writer, c.Request.Body, maxIngestBodyBytes)
	defer func() {
		_ = body.Close()
	}()

	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()

	var request IngestMetricsRequest
	if err := decoder.Decode(&request); err != nil {
		return IngestMetricsRequest{}, err
	}

	var extra struct{}
	if err := decoder.Decode(&extra); err != io.EOF {
		return IngestMetricsRequest{}, errors.New("request body must contain a single JSON object")
	}

	return request, nil
}

func hasJSONContentType(header string) bool {
	mediaType, _, err := mime.ParseMediaType(header)
	if err != nil {
		return false
	}

	return mediaType == jsonContentType
}
