package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDecodeAndExpandIngestMessage(t *testing.T) {
	body := validIngestBody(t, "message-1", "task-1", []IngestSample{{
		Step: 7,
		TS:   1756089600123,
		Metrics: map[string]float64{
			"loss": 1.25,
			"lr":   0.001,
		},
	}})

	message, err := DecodeIngestMessage(body)
	if err != nil {
		t.Fatalf("DecodeIngestMessage() error = %v", err)
	}
	points := ExpandMetricPoints(message)
	if len(points) != 2 {
		t.Fatalf("expanded points = %d, want 2", len(points))
	}
	if points[0].Key != "loss" || points[0].Value != 1.25 {
		t.Fatalf("first point = %#v, want sorted loss point", points[0])
	}
	if points[1].Key != "lr" || points[1].Value != 0.001 {
		t.Fatalf("second point = %#v, want sorted lr point", points[1])
	}
}

func TestConsumerDefaultsMatchWorkerBatchingContract(t *testing.T) {
	config := (ConsumerConfig{URL: "amqp://test/"}).withDefaults()
	if config.BatchMax != 500 {
		t.Fatalf("BatchMax = %d, want 500", config.BatchMax)
	}
	if config.Prefetch != 500 {
		t.Fatalf("Prefetch = %d, want 500", config.Prefetch)
	}
	if config.FlushInterval != 100*time.Millisecond {
		t.Fatalf("FlushInterval = %s, want 100ms", config.FlushInterval)
	}
}

func TestDecodeIngestMessageRejectsProtocolViolations(t *testing.T) {
	tests := map[string]string{
		"malformed json":      `{`,
		"unknown version":     `{"schema_version":2,"message_id":"m","correlation_id":"m","task_id":"t","batch":[{"step":1,"ts":1,"metrics":{"loss":1}}]}`,
		"unknown field":       `{"schema_version":1,"message_id":"m","correlation_id":"m","task_id":"t","batch":[{"step":1,"ts":1,"metrics":{"loss":1}}],"extra":true}`,
		"missing metric data": `{"schema_version":1,"message_id":"m","correlation_id":"m","task_id":"t","batch":[{"step":1,"ts":1,"metrics":{}}]}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeIngestMessage([]byte(body)); err == nil {
				t.Fatal("DecodeIngestMessage() error = nil, want protocol error")
			}
		})
	}
}

func TestConsumerConfiguresManualAckAndQoS(t *testing.T) {
	session := newFakeConsumerSession()
	consumer := newTestConsumer(t, ConsumerConfig{
		URL:            "amqp://test/",
		Prefetch:       23,
		sessionFactory: func(context.Context, string) (consumerSession, error) { return session, nil },
	}, MetricPointSinkFunc(func(context.Context, []domain.MetricPoint) error { return nil }), discardLogger())

	opened, _, err := consumer.openSession(context.Background())
	if err != nil {
		t.Fatalf("openSession() error = %v", err)
	}
	defer func() { _ = opened.Close() }()
	if session.qosPrefetch != 23 {
		t.Fatalf("Qos prefetch = %d, want 23", session.qosPrefetch)
	}
	if session.consumeAutoAck {
		t.Fatal("Consume autoAck = true, want manual ack")
	}
	if session.consumeQueue != IngestQueue {
		t.Fatalf("Consume queue = %q, want %q", session.consumeQueue, IngestQueue)
	}
}

func TestConsumerFlushesAtBatchMaxAndAcksAfterAllDeliveryPartsCommit(t *testing.T) {
	acknowledger := &recordingAcknowledger{}
	delivery := validDelivery(t, acknowledger, 1, "message-1", "task-1", samplesWithPointCount(600))
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- delivery

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	flushes := make(chan []domain.MetricPoint, 2)
	consumer := newTestConsumer(t, ConsumerConfig{
		URL:             "amqp://test/",
		BatchMax:        500,
		FlushInterval:   time.Hour,
		ShutdownTimeout: time.Second,
	}, MetricPointSinkFunc(func(_ context.Context, points []domain.MetricPoint) error {
		copied := append([]domain.MetricPoint(nil), points...)
		flushes <- copied
		if len(points) == 500 {
			if got := acknowledger.ackCount(); got != 0 {
				t.Errorf("acks after first delivery part = %d, want 0", got)
			}
			cancel()
		}
		return nil
	}), discardLogger())

	done := make(chan error, 1)
	go func() { done <- consumer.consumeSession(ctx, deliveries) }()

	first := receiveFlush(t, flushes)
	if len(first) != 500 {
		t.Fatalf("first flush rows = %d, want 500", len(first))
	}
	second := receiveFlush(t, flushes)
	if len(second) != 100 {
		t.Fatalf("shutdown flush rows = %d, want 100", len(second))
	}
	if err := receiveDone(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("consumeSession() error = %v, want context canceled", err)
	}
	if got := acknowledger.ackCount(); got != 1 {
		t.Fatalf("acks after all delivery parts = %d, want 1", got)
	}
}

func TestConsumerFlushesPartialBatchOneIntervalAfterFirstPoint(t *testing.T) {
	acknowledger := &recordingAcknowledger{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- validDelivery(t, acknowledger, 1, "message-1", "task-1", samplesWithPointCount(1))
	flushed := make(chan time.Time, 1)
	ctx, cancel := context.WithCancel(context.Background())
	consumer := newTestConsumer(t, ConsumerConfig{
		URL:           "amqp://test/",
		BatchMax:      500,
		FlushInterval: 30 * time.Millisecond,
	}, MetricPointSinkFunc(func(context.Context, []domain.MetricPoint) error {
		flushed <- time.Now()
		cancel()
		return nil
	}), discardLogger())

	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- consumer.consumeSession(ctx, deliveries) }()

	flushTime := receiveTime(t, flushed)
	elapsed := flushTime.Sub(started)
	if elapsed < 20*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("flush elapsed = %s, want approximately 30ms", elapsed)
	}
	_ = receiveDone(t, done)
	if got := acknowledger.ackCount(); got != 1 {
		t.Fatalf("acks = %d, want 1", got)
	}
}

func TestConsumerRequeuesEveryDeliveryAssociatedWithFailedFlush(t *testing.T) {
	firstAck := &recordingAcknowledger{}
	secondAck := &recordingAcknowledger{}
	deliveries := make(chan amqp.Delivery, 2)
	deliveries <- validDelivery(t, firstAck, 1, "message-1", "task-1", samplesWithPointCount(1))
	deliveries <- validDelivery(t, secondAck, 2, "message-2", "task-2", samplesWithPointCount(1))
	ctx, cancel := context.WithCancel(context.Background())
	consumer := newTestConsumer(t, ConsumerConfig{
		URL:            "amqp://test/",
		BatchMax:       2,
		FlushInterval:  time.Hour,
		FailureBackoff: time.Millisecond,
		sleep:          func(context.Context, time.Duration) error { return nil },
	}, MetricPointSinkFunc(func(context.Context, []domain.MetricPoint) error {
		cancel()
		return errors.New("database unavailable")
	}), discardLogger())

	done := make(chan error, 1)
	go func() { done <- consumer.consumeSession(ctx, deliveries) }()
	_ = receiveDone(t, done)

	assertSingleNack(t, firstAck, true)
	assertSingleNack(t, secondAck, true)
	if firstAck.ackCount() != 0 || secondAck.ackCount() != 0 {
		t.Fatal("failed flush acknowledged a delivery")
	}
}

func TestConsumerIncreasesFailureBackoffAndResetsItAfterSuccess(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 4)
	for index := 1; index <= 4; index++ {
		deliveries <- validDelivery(t, &recordingAcknowledger{}, uint64(index), "message-"+string(rune('0'+index)), "task-1", samplesWithPointCount(1))
	}
	ctx, cancel := context.WithCancel(context.Background())
	var waits []time.Duration
	flushCount := 0
	consumer := newTestConsumer(t, ConsumerConfig{
		URL:               "amqp://test/",
		BatchMax:          1,
		FlushInterval:     time.Hour,
		FailureBackoff:    100 * time.Millisecond,
		FailureMaxBackoff: 250 * time.Millisecond,
		sleep: func(_ context.Context, duration time.Duration) error {
			waits = append(waits, duration)
			return nil
		},
	}, MetricPointSinkFunc(func(context.Context, []domain.MetricPoint) error {
		flushCount++
		switch flushCount {
		case 1, 2, 4:
			if flushCount == 4 {
				cancel()
			}
			return errors.New("database unavailable")
		default:
			return nil
		}
	}), discardLogger())

	done := make(chan error, 1)
	go func() { done <- consumer.consumeSession(ctx, deliveries) }()
	_ = receiveDone(t, done)

	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 100 * time.Millisecond}
	if len(waits) != len(want) {
		t.Fatalf("failure waits = %v, want %v", waits, want)
	}
	for index := range want {
		if waits[index] != want[index] {
			t.Fatalf("failure waits[%d] = %s, want %s", index, waits[index], want[index])
		}
	}
}

func TestConsumerDeadLettersProtocolErrorAndWritesStructuredLog(t *testing.T) {
	acknowledger := &recordingAcknowledger{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  9,
		ContentType:  "application/json",
		MessageId:    "broken-message",
		Body:         []byte(`{"schema_version":1`),
	}
	close(deliveries)

	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	consumer := newTestConsumer(t, ConsumerConfig{URL: "amqp://test/"}, MetricPointSinkFunc(func(context.Context, []domain.MetricPoint) error {
		t.Fatal("sink called for protocol error")
		return nil
	}), logger)

	err := consumer.consumeSession(context.Background(), deliveries)
	if !errors.Is(err, ErrDeliveryStreamClosed) {
		t.Fatalf("consumeSession() error = %v, want stream closed", err)
	}
	assertSingleNack(t, acknowledger, false)

	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("decode structured log: %v", err)
	}
	if entry["error_class"] != "message_protocol" || entry["message_id"] != "broken-message" || entry["delivery_tag"] != float64(9) {
		t.Fatalf("structured log fields = %#v", entry)
	}
}

func newTestConsumer(t *testing.T, config ConsumerConfig, sink MetricPointSink, logger *slog.Logger) *RabbitMQMetricConsumer {
	t.Helper()
	config.jitter = func(duration time.Duration) time.Duration { return duration }
	consumer, err := NewRabbitMQMetricConsumer(config, sink, logger)
	if err != nil {
		t.Fatalf("NewRabbitMQMetricConsumer() error = %v", err)
	}
	return consumer
}

func validDelivery(t *testing.T, acknowledger amqp.Acknowledger, tag uint64, messageID string, taskID string, samples []IngestSample) amqp.Delivery {
	t.Helper()
	body := validIngestBody(t, messageID, taskID, samples)
	return amqp.Delivery{
		Acknowledger: acknowledger,
		Headers: amqp.Table{
			"schema_version": int32(IngestSchemaVersion),
			"message_id":     messageID,
			"correlation_id": messageID,
		},
		ContentType:   "application/json",
		CorrelationId: messageID,
		MessageId:     messageID,
		DeliveryTag:   tag,
		Body:          body,
	}
}

func validIngestBody(t *testing.T, messageID string, taskID string, samples []IngestSample) []byte {
	t.Helper()
	body, err := json.Marshal(IngestMessage{
		SchemaVersion: IngestSchemaVersion,
		MessageID:     messageID,
		CorrelationID: messageID,
		TaskID:        taskID,
		Batch:         samples,
	})
	if err != nil {
		t.Fatalf("marshal ingest message: %v", err)
	}
	return body
}

func samplesWithPointCount(count int) []IngestSample {
	samples := make([]IngestSample, 0, (count+1)/2)
	for index := 0; index < count; index += 2 {
		metrics := map[string]float64{"loss": float64(index)}
		if index+1 < count {
			metrics["lr"] = float64(index + 1)
		}
		samples = append(samples, IngestSample{
			Step:    int64(index / 2),
			TS:      1756089600000 + int64(index),
			Metrics: metrics,
		})
	}
	return samples
}

func receiveFlush(t *testing.T, flushes <-chan []domain.MetricPoint) []domain.MetricPoint {
	t.Helper()
	select {
	case points := <-flushes:
		return points
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for flush")
		return nil
	}
}

func receiveTime(t *testing.T, times <-chan time.Time) time.Time {
	t.Helper()
	select {
	case value := <-times:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for time")
		return time.Time{}
	}
}

func receiveDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for consumer")
		return nil
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func assertSingleNack(t *testing.T, acknowledger *recordingAcknowledger, requeue bool) {
	t.Helper()
	acknowledger.mu.Lock()
	defer acknowledger.mu.Unlock()
	if len(acknowledger.nacks) != 1 {
		t.Fatalf("nacks = %d, want 1", len(acknowledger.nacks))
	}
	if acknowledger.nacks[0].requeue != requeue || acknowledger.nacks[0].multiple {
		t.Fatalf("nack = %#v, want multiple=false requeue=%t", acknowledger.nacks[0], requeue)
	}
}

type recordingAcknowledger struct {
	mu    sync.Mutex
	acks  []ackCall
	nacks []nackCall
}

func (a *recordingAcknowledger) Ack(tag uint64, multiple bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acks = append(a.acks, ackCall{tag: tag, multiple: multiple})
	return nil
}

func (a *recordingAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nacks = append(a.nacks, nackCall{tag: tag, multiple: multiple, requeue: requeue})
	return nil
}

func (a *recordingAcknowledger) Reject(tag uint64, requeue bool) error {
	return a.Nack(tag, false, requeue)
}

func (a *recordingAcknowledger) ackCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.acks)
}

type ackCall struct {
	tag      uint64
	multiple bool
}

type nackCall struct {
	tag      uint64
	multiple bool
	requeue  bool
}

type fakeConsumerSession struct {
	deliveries     chan amqp.Delivery
	qosPrefetch    int
	consumeQueue   string
	consumeAutoAck bool
	closed         bool
}

func newFakeConsumerSession() *fakeConsumerSession {
	return &fakeConsumerSession{deliveries: make(chan amqp.Delivery)}
}

func (s *fakeConsumerSession) ExchangeDeclare(string, string, bool, bool, bool, bool, amqp.Table) error {
	return nil
}

func (s *fakeConsumerSession) QueueDeclare(name string, _ bool, _ bool, _ bool, _ bool, _ amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: name}, nil
}

func (s *fakeConsumerSession) QueueBind(string, string, string, bool, amqp.Table) error { return nil }

func (s *fakeConsumerSession) Qos(prefetchCount int, _ int, _ bool) error {
	s.qosPrefetch = prefetchCount
	return nil
}

func (s *fakeConsumerSession) Consume(queue string, _ string, autoAck bool, _ bool, _ bool, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	s.consumeQueue = queue
	s.consumeAutoAck = autoAck
	return s.deliveries, nil
}

func (s *fakeConsumerSession) Close() error {
	s.closed = true
	return nil
}
