package messaging

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

const IngestSchemaVersion = 1

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
