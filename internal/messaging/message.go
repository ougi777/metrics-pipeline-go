package messaging

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

const IngestSchemaVersion = 1

const (
	maxIngestBatchSize = 500
	maxTaskIDLength    = 64
	maxMetricKeyLength = 32
	maxStepValue       = math.MaxInt32
	maxTimestampMillis = 253402300799999
)

var (
	taskIDPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	metricKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

type IDGenerator func() (string, error)

type IngestMessage struct {
	SchemaVersion int            `json:"schema_version"`
	MessageID     string         `json:"message_id"`
	CorrelationID string         `json:"correlation_id"`
	TaskID        string         `json:"task_id"`
	Batch         []IngestSample `json:"batch"`
}

type IngestSample struct {
	Step    int64              `json:"step"`
	TS      int64              `json:"ts"`
	Metrics map[string]float64 `json:"metrics"`
}

type ingestMessageWire struct {
	SchemaVersion *int               `json:"schema_version"`
	MessageID     string             `json:"message_id"`
	CorrelationID string             `json:"correlation_id"`
	TaskID        string             `json:"task_id"`
	Batch         []ingestSampleWire `json:"batch"`
}

type ingestSampleWire struct {
	Step    *int64             `json:"step"`
	TS      *int64             `json:"ts"`
	Metrics map[string]float64 `json:"metrics"`
}

func NewIngestMessage(batch domain.MetricBatch, generateID IDGenerator) (IngestMessage, error) {
	if generateID == nil {
		generateID = GenerateMessageID
	}

	messageID, err := generateID()
	if err != nil {
		return IngestMessage{}, fmt.Errorf("generate message id: %w", err)
	}

	samples := make([]IngestSample, 0, len(batch.Samples))
	for _, sample := range batch.Samples {
		metrics := make(map[string]float64, len(sample.Metrics))
		for key, value := range sample.Metrics {
			metrics[key] = value
		}
		samples = append(samples, IngestSample{
			Step:    sample.Step,
			TS:      sample.TimestampMillis,
			Metrics: metrics,
		})
	}

	return IngestMessage{
		SchemaVersion: IngestSchemaVersion,
		MessageID:     messageID,
		CorrelationID: messageID,
		TaskID:        batch.TaskID,
		Batch:         samples,
	}, nil
}

func GenerateMessageID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes[:]), nil
}

func MarshalIngestPublishing(message IngestMessage, now time.Time) (amqp.Publishing, error) {
	body, err := json.Marshal(message)
	if err != nil {
		return amqp.Publishing{}, fmt.Errorf("marshal ingest message: %w", err)
	}

	return amqp.Publishing{
		Headers: amqp.Table{
			"schema_version": int32(message.SchemaVersion),
			"message_id":     message.MessageID,
			"correlation_id": message.CorrelationID,
		},
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		Priority:      0,
		CorrelationId: message.CorrelationID,
		MessageId:     message.MessageID,
		Timestamp:     now.UTC(),
		Body:          body,
	}, nil
}

// DecodeIngestMessage 严格解析并校验 worker 收到的内部消息协议。
func DecodeIngestMessage(body []byte) (IngestMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()

	var wire ingestMessageWire
	if err := decoder.Decode(&wire); err != nil {
		return IngestMessage{}, fmt.Errorf("decode ingest message: %w", err)
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return IngestMessage{}, errors.New("decode ingest message: body must contain one JSON object")
	}
	if wire.SchemaVersion == nil {
		return IngestMessage{}, errors.New("validate ingest message: schema_version is required")
	}
	if *wire.SchemaVersion != IngestSchemaVersion {
		return IngestMessage{}, fmt.Errorf("validate ingest message: unsupported schema_version %d", *wire.SchemaVersion)
	}
	if wire.MessageID == "" {
		return IngestMessage{}, errors.New("validate ingest message: message_id is required")
	}
	if wire.CorrelationID == "" {
		return IngestMessage{}, errors.New("validate ingest message: correlation_id is required")
	}
	if wire.TaskID == "" || len(wire.TaskID) > maxTaskIDLength || !taskIDPattern.MatchString(wire.TaskID) {
		return IngestMessage{}, errors.New("validate ingest message: task_id is invalid")
	}
	if len(wire.Batch) == 0 || len(wire.Batch) > maxIngestBatchSize {
		return IngestMessage{}, fmt.Errorf("validate ingest message: batch must contain 1 to %d samples", maxIngestBatchSize)
	}

	message := IngestMessage{
		SchemaVersion: *wire.SchemaVersion,
		MessageID:     wire.MessageID,
		CorrelationID: wire.CorrelationID,
		TaskID:        wire.TaskID,
		Batch:         make([]IngestSample, 0, len(wire.Batch)),
	}
	for index, sample := range wire.Batch {
		if sample.Step == nil || *sample.Step < 0 || *sample.Step > maxStepValue {
			return IngestMessage{}, fmt.Errorf("validate ingest message: batch[%d].step is invalid", index)
		}
		if sample.TS == nil || *sample.TS <= 0 || *sample.TS > maxTimestampMillis {
			return IngestMessage{}, fmt.Errorf("validate ingest message: batch[%d].ts is invalid", index)
		}
		if len(sample.Metrics) == 0 {
			return IngestMessage{}, fmt.Errorf("validate ingest message: batch[%d].metrics is empty", index)
		}
		metrics := make(map[string]float64, len(sample.Metrics))
		for key, value := range sample.Metrics {
			if key == "" || len(key) > maxMetricKeyLength || !metricKeyPattern.MatchString(key) {
				return IngestMessage{}, fmt.Errorf("validate ingest message: batch[%d].metrics key %q is invalid", index, key)
			}
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return IngestMessage{}, fmt.Errorf("validate ingest message: batch[%d].metrics[%q] must be finite", index, key)
			}
			metrics[key] = value
		}
		message.Batch = append(message.Batch, IngestSample{
			Step:    *sample.Step,
			TS:      *sample.TS,
			Metrics: metrics,
		})
	}

	return message, nil
}

// ExpandMetricPoints 将每个 metrics key-value 稳定地展开为一条指标记录。
func ExpandMetricPoints(message IngestMessage) []domain.MetricPoint {
	pointCount := 0
	for _, sample := range message.Batch {
		pointCount += len(sample.Metrics)
	}
	points := make([]domain.MetricPoint, 0, pointCount)
	for _, sample := range message.Batch {
		keys := make([]string, 0, len(sample.Metrics))
		for key := range sample.Metrics {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			points = append(points, domain.MetricPoint{
				TaskID:          message.TaskID,
				Key:             key,
				Step:            sample.Step,
				TimestampMillis: sample.TS,
				Value:           sample.Metrics[key],
			})
		}
	}

	return points
}
