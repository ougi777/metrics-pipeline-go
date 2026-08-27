package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQPublisherDeclaresDurableTopologyAndEnablesConfirm(t *testing.T) {
	session := newFakeAMQPSession()
	publisher := newTestPublisher(t, session)
	defer closeTestPublisher(t, publisher)

	if len(session.exchangeDeclares) != 2 {
		t.Fatalf("exchange declares = %d, want 2", len(session.exchangeDeclares))
	}
	if session.exchangeDeclares[0].name != DeadLetterExchange {
		t.Fatalf("first exchange = %q, want %q", session.exchangeDeclares[0].name, DeadLetterExchange)
	}
	if session.exchangeDeclares[1].name != IngestExchange {
		t.Fatalf("second exchange = %q, want %q", session.exchangeDeclares[1].name, IngestExchange)
	}
	for _, call := range session.exchangeDeclares {
		if !call.durable {
			t.Fatalf("exchange %q durable = false, want true", call.name)
		}
		if call.kind != IngestExchangeKind {
			t.Fatalf("exchange %q kind = %q, want %q", call.name, call.kind, IngestExchangeKind)
		}
	}

	if len(session.queueDeclares) != 2 {
		t.Fatalf("queue declares = %d, want 2", len(session.queueDeclares))
	}
	if session.queueDeclares[0].name != DeadLetterQueue {
		t.Fatalf("first queue = %q, want %q", session.queueDeclares[0].name, DeadLetterQueue)
	}
	if session.queueDeclares[1].name != IngestQueue {
		t.Fatalf("second queue = %q, want %q", session.queueDeclares[1].name, IngestQueue)
	}
	for _, call := range session.queueDeclares {
		if !call.durable {
			t.Fatalf("queue %q durable = false, want true", call.name)
		}
	}
	args := session.queueDeclares[1].args
	if args["x-dead-letter-exchange"] != DeadLetterExchange {
		t.Fatalf("dead letter exchange = %v, want %s", args["x-dead-letter-exchange"], DeadLetterExchange)
	}
	if args["x-dead-letter-routing-key"] != DeadLetterRoutingKey {
		t.Fatalf("dead letter routing key = %v, want %s", args["x-dead-letter-routing-key"], DeadLetterRoutingKey)
	}

	if len(session.queueBinds) != 2 {
		t.Fatalf("queue binds = %d, want 2", len(session.queueBinds))
	}
	if session.queueBinds[1].name != IngestQueue || session.queueBinds[1].key != IngestRoutingKey || session.queueBinds[1].exchange != IngestExchange {
		t.Fatalf("ingest bind = %#v, want queue/key/exchange %s/%s/%s", session.queueBinds[1], IngestQueue, IngestRoutingKey, IngestExchange)
	}
	if !session.confirmEnabled {
		t.Fatal("confirm mode disabled, want enabled")
	}
}

func TestRabbitMQPublisherPublishesPersistentMessageAfterConfirmAck(t *testing.T) {
	session := newFakeAMQPSession()
	session.onPublish = func() {
		session.confirmReceiver <- amqp.Confirmation{Ack: true}
	}
	publisher := newTestPublisher(t, session)
	defer closeTestPublisher(t, publisher)

	if err := publisher.PublishMetricBatch(context.Background(), sampleMetricBatch()); err != nil {
		t.Fatalf("PublishMetricBatch() error = %v", err)
	}

	if len(session.publishCalls) != 1 {
		t.Fatalf("publish calls = %d, want 1", len(session.publishCalls))
	}
	call := session.publishCalls[0]
	if call.exchange != IngestExchange {
		t.Fatalf("exchange = %q, want %q", call.exchange, IngestExchange)
	}
	if call.key != IngestRoutingKey {
		t.Fatalf("routing key = %q, want %q", call.key, IngestRoutingKey)
	}
	if !call.mandatory {
		t.Fatal("mandatory = false, want true")
	}
	if call.immediate {
		t.Fatal("immediate = true, want false")
	}
	if call.message.DeliveryMode != amqp.Persistent {
		t.Fatalf("delivery mode = %d, want persistent", call.message.DeliveryMode)
	}
	if call.message.ContentType != "application/json" {
		t.Fatalf("content type = %q, want application/json", call.message.ContentType)
	}
	if call.message.MessageId != "message-1" {
		t.Fatalf("message id = %q, want message-1", call.message.MessageId)
	}
	if call.message.CorrelationId != "message-1" {
		t.Fatalf("correlation id = %q, want message-1", call.message.CorrelationId)
	}
	if call.message.Headers["schema_version"] != int32(IngestSchemaVersion) {
		t.Fatalf("schema_version header = %v, want %d", call.message.Headers["schema_version"], IngestSchemaVersion)
	}

	var body IngestMessage
	if err := json.Unmarshal(call.message.Body, &body); err != nil {
		t.Fatalf("Unmarshal(body) error = %v", err)
	}
	if body.SchemaVersion != IngestSchemaVersion {
		t.Fatalf("schema version = %d, want %d", body.SchemaVersion, IngestSchemaVersion)
	}
	if body.TaskID != "ft-20260825-0001" {
		t.Fatalf("task_id = %q, want ft-20260825-0001", body.TaskID)
	}
	if len(body.Batch) != 1 {
		t.Fatalf("batch length = %d, want 1", len(body.Batch))
	}
	if body.Batch[0].Metrics["loss"] != 1.2 {
		t.Fatalf("loss = %f, want 1.2", body.Batch[0].Metrics["loss"])
	}
}

func TestRabbitMQPublisherLogsMessageIdentityAndAttemptResult(t *testing.T) {
	session := newFakeAMQPSession()
	session.onPublish = func() {
		session.confirmReceiver <- amqp.Confirmation{DeliveryTag: 7, Ack: true}
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	publisher, err := NewRabbitMQMetricBatchPublisher(context.Background(), testPublisherConfig(sequenceSessionFactory(session)), logger)
	if err != nil {
		t.Fatalf("NewRabbitMQMetricBatchPublisher() error = %v", err)
	}
	defer closeTestPublisher(t, publisher)

	if err := publisher.PublishMetricBatch(context.Background(), sampleMetricBatch()); err != nil {
		t.Fatalf("PublishMetricBatch() error = %v", err)
	}

	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("Unmarshal(log) error = %v; log: %s", err, output.String())
	}
	assertLogField(t, entry, "task_id", "ft-20260825-0001")
	assertLogField(t, entry, "message_id", "message-1")
	assertLogField(t, entry, "delivery_tag", float64(7))
	assertLogField(t, entry, "attempt", float64(1))
	assertLogField(t, entry, "error_class", "none")
	if _, ok := entry["duration_ms"]; !ok {
		t.Fatal("log has no duration_ms field")
	}
}

func TestRabbitMQPublisherRetriesWithFreshSessionAfterPublishFailure(t *testing.T) {
	firstSession := newFakeAMQPSession()
	firstSession.publishErr = errors.New("connection reset")
	secondSession := newFakeAMQPSession()
	secondSession.onPublish = func() {
		secondSession.confirmReceiver <- amqp.Confirmation{Ack: true}
	}
	factory := sequenceSessionFactory(firstSession, secondSession)
	publisher := newTestPublisherWithConfig(t, PublisherConfig{
		URL:            "amqp://test/",
		Publishers:     1,
		ConfirmTimeout: 50 * time.Millisecond,
		MaxAttempts:    2,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		IDGenerator:    fixedIDGenerator("message-1"),
		Clock:          fixedClock,
		sessionFactory: factory,
		sleep:          immediateSleep,
	})
	defer closeTestPublisher(t, publisher)

	if err := publisher.PublishMetricBatch(context.Background(), sampleMetricBatch()); err != nil {
		t.Fatalf("PublishMetricBatch() error = %v", err)
	}
	if len(firstSession.publishCalls) != 1 {
		t.Fatalf("first session publish calls = %d, want 1", len(firstSession.publishCalls))
	}
	if !firstSession.closed {
		t.Fatal("first session closed = false, want true after publish failure")
	}
	if len(secondSession.publishCalls) != 1 {
		t.Fatalf("second session publish calls = %d, want 1", len(secondSession.publishCalls))
	}
	if len(secondSession.exchangeDeclares) != 2 {
		t.Fatalf("second session exchange declares = %d, want topology redeclared", len(secondSession.exchangeDeclares))
	}
	if !secondSession.confirmEnabled {
		t.Fatal("second session confirm mode disabled, want enabled")
	}
	closeTestPublisher(t, publisher)
	if !secondSession.closed {
		t.Fatal("second session closed = false, want true after publisher close")
	}
}

func TestRabbitMQPublisherReturnsErrorAfterBrokerNack(t *testing.T) {
	session := newFakeAMQPSession()
	session.onPublish = func() {
		session.confirmReceiver <- amqp.Confirmation{Ack: false}
	}
	publisher := newTestPublisherWithConfig(t, testPublisherConfig(sequenceSessionFactory(session)))
	defer closeTestPublisher(t, publisher)

	err := publisher.PublishMetricBatch(context.Background(), sampleMetricBatch())
	if !errors.Is(err, ErrBrokerNack) {
		t.Fatalf("PublishMetricBatch() error = %v, want broker nack", err)
	}
	if !errors.Is(err, ErrPublishFailed) {
		t.Fatalf("PublishMetricBatch() error = %v, want publish failed", err)
	}
}

func TestRabbitMQPublisherReturnsErrorAfterMandatoryReturn(t *testing.T) {
	for range 64 {
		func() {
			session := newFakeAMQPSession()
			session.onPublish = func() {
				session.returnReceiver <- amqp.Return{
					ReplyCode:  312,
					ReplyText:  "NO_ROUTE",
					Exchange:   IngestExchange,
					RoutingKey: IngestRoutingKey,
				}
				session.confirmReceiver <- amqp.Confirmation{Ack: true}
			}
			publisher := newTestPublisherWithConfig(t, testPublisherConfig(sequenceSessionFactory(session)))
			defer closeTestPublisher(t, publisher)

			err := publisher.PublishMetricBatch(context.Background(), sampleMetricBatch())
			if !errors.Is(err, ErrUnroutable) {
				t.Fatalf("PublishMetricBatch() error = %v, want unroutable", err)
			}
			if !errors.Is(err, ErrPublishFailed) {
				t.Fatalf("PublishMetricBatch() error = %v, want publish failed", err)
			}
		}()
	}
}

func TestRabbitMQPublisherReturnsErrorAfterConfirmTimeout(t *testing.T) {
	session := newFakeAMQPSession()
	publisher := newTestPublisherWithConfig(t, PublisherConfig{
		URL:            "amqp://test/",
		Publishers:     1,
		ConfirmTimeout: time.Nanosecond,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		IDGenerator:    fixedIDGenerator("message-1"),
		Clock:          fixedClock,
		sessionFactory: sequenceSessionFactory(session),
		sleep:          immediateSleep,
	})
	defer closeTestPublisher(t, publisher)

	err := publisher.PublishMetricBatch(context.Background(), sampleMetricBatch())
	if !errors.Is(err, ErrConfirmTimeout) {
		t.Fatalf("PublishMetricBatch() error = %v, want confirm timeout", err)
	}
	if !errors.Is(err, ErrPublishFailed) {
		t.Fatalf("PublishMetricBatch() error = %v, want publish failed", err)
	}
}

func TestWriteDeadlineConnTimesOutBlockedWrite(t *testing.T) {
	client, server := net.Pipe()
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()

	connection := &writeDeadlineConn{Conn: client, timeout: 20 * time.Millisecond}
	started := time.Now()
	_, err := connection.Write([]byte("blocked"))
	if err == nil {
		t.Fatal("Write() error = nil, want timeout")
	}
	networkError, ok := err.(net.Error)
	if !ok || !networkError.Timeout() {
		t.Fatalf("Write() error = %v, want network timeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Write() elapsed = %s, want at most 1s", elapsed)
	}
}

func TestRabbitMQPublisherReturnsErrorWhenRetryBudgetIsExhausted(t *testing.T) {
	firstSession := newFakeAMQPSession()
	firstSession.publishErr = errors.New("first failure")
	secondSession := newFakeAMQPSession()
	secondSession.publishErr = errors.New("second failure")
	publisher := newTestPublisherWithConfig(t, PublisherConfig{
		URL:            "amqp://test/",
		Publishers:     1,
		ConfirmTimeout: 50 * time.Millisecond,
		MaxAttempts:    2,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		IDGenerator:    fixedIDGenerator("message-1"),
		Clock:          fixedClock,
		sessionFactory: sequenceSessionFactory(firstSession, secondSession),
		sleep:          immediateSleep,
	})
	defer closeTestPublisher(t, publisher)

	err := publisher.PublishMetricBatch(context.Background(), sampleMetricBatch())
	if !errors.Is(err, ErrPublishFailed) {
		t.Fatalf("PublishMetricBatch() error = %v, want publish failed", err)
	}
	if len(firstSession.publishCalls) != 1 {
		t.Fatalf("first session publish calls = %d, want 1", len(firstSession.publishCalls))
	}
	if len(secondSession.publishCalls) != 1 {
		t.Fatalf("second session publish calls = %d, want 1", len(secondSession.publishCalls))
	}
}

func TestRabbitMQPublisherCloseCancelsBlockedReconnect(t *testing.T) {
	firstSession := newFakeAMQPSession()
	firstSession.publishErr = errors.New("connection reset")
	reconnectStarted := make(chan struct{})
	factoryCalls := 0
	factory := func(ctx context.Context, _ string) (amqpSession, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return firstSession, nil
		}

		close(reconnectStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	publisher := newTestPublisherWithConfig(t, PublisherConfig{
		URL:            "amqp://test/",
		Publishers:     1,
		ConfirmTimeout: 50 * time.Millisecond,
		MaxAttempts:    2,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		IDGenerator:    fixedIDGenerator("message-1"),
		Clock:          fixedClock,
		sessionFactory: factory,
		sleep:          immediateSleep,
	})

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- publisher.PublishMetricBatch(context.Background(), sampleMetricBatch())
	}()

	select {
	case <-reconnectStarted:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not start")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- publisher.Close()
	}()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() blocked while reconnecting")
	}

	select {
	case <-publishDone:
	case <-time.After(time.Second):
		t.Fatal("PublishMetricBatch() remained blocked after Close()")
	}
}

func newTestPublisher(t *testing.T, session *fakeAMQPSession) *RabbitMQMetricBatchPublisher {
	t.Helper()

	return newTestPublisherWithConfig(t, testPublisherConfig(sequenceSessionFactory(session)))
}

func newTestPublisherWithConfig(t *testing.T, config PublisherConfig) *RabbitMQMetricBatchPublisher {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	publisher, err := NewRabbitMQMetricBatchPublisher(context.Background(), config, logger)
	if err != nil {
		t.Fatalf("NewRabbitMQMetricBatchPublisher() error = %v", err)
	}

	return publisher
}

func closeTestPublisher(t *testing.T, publisher *RabbitMQMetricBatchPublisher) {
	t.Helper()

	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func assertLogField(t *testing.T, entry map[string]any, name string, want any) {
	t.Helper()

	if got := entry[name]; got != want {
		t.Fatalf("log field %s = %#v, want %#v", name, got, want)
	}
}

func testPublisherConfig(factory amqpSessionFactory) PublisherConfig {
	return PublisherConfig{
		URL:            "amqp://test/",
		Publishers:     1,
		WriteTimeout:   50 * time.Millisecond,
		ConfirmTimeout: 50 * time.Millisecond,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		IDGenerator:    fixedIDGenerator("message-1"),
		Clock:          fixedClock,
		sessionFactory: factory,
		sleep:          immediateSleep,
	}
}

func sampleMetricBatch() domain.MetricBatch {
	return domain.MetricBatch{
		TaskID: "ft-20260825-0001",
		Samples: []domain.MetricSample{
			{
				Step:            120,
				TimestampMillis: 1756089600123,
				Metrics:         map[string]float64{"loss": 1.2, "lr": 0.00003},
			},
		},
	}
}

func fixedIDGenerator(value string) IDGenerator {
	return func() (string, error) {
		return value, nil
	}
}

func fixedClock() time.Time {
	return time.Date(2026, 8, 27, 1, 2, 3, 0, time.UTC)
}

func immediateSleep(context.Context, time.Duration) error {
	return nil
}

func sequenceSessionFactory(sessions ...*fakeAMQPSession) amqpSessionFactory {
	index := 0

	return func(context.Context, string) (amqpSession, error) {
		if index >= len(sessions) {
			return nil, errors.New("no fake sessions remain")
		}

		session := sessions[index]
		index++
		return session, nil
	}
}

type fakeAMQPSession struct {
	exchangeDeclares []exchangeDeclareCall
	queueDeclares    []queueDeclareCall
	queueBinds       []queueBindCall
	publishCalls     []publishCall
	confirmEnabled   bool
	confirmReceiver  chan amqp.Confirmation
	returnReceiver   chan amqp.Return
	publishErr       error
	onPublish        func()
	closed           bool
}

func newFakeAMQPSession() *fakeAMQPSession {
	return &fakeAMQPSession{}
}

func (s *fakeAMQPSession) ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error {
	s.exchangeDeclares = append(s.exchangeDeclares, exchangeDeclareCall{
		name:       name,
		kind:       kind,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		noWait:     noWait,
		args:       args,
	})

	return nil
}

func (s *fakeAMQPSession) QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error) {
	s.queueDeclares = append(s.queueDeclares, queueDeclareCall{
		name:       name,
		durable:    durable,
		autoDelete: autoDelete,
		exclusive:  exclusive,
		noWait:     noWait,
		args:       args,
	})

	return amqp.Queue{Name: name}, nil
}

func (s *fakeAMQPSession) QueueBind(name string, key string, exchange string, noWait bool, args amqp.Table) error {
	s.queueBinds = append(s.queueBinds, queueBindCall{
		name:     name,
		key:      key,
		exchange: exchange,
		noWait:   noWait,
		args:     args,
	})

	return nil
}

func (s *fakeAMQPSession) Confirm(noWait bool) error {
	s.confirmEnabled = !noWait
	return nil
}

func (s *fakeAMQPSession) NotifyPublish(receiver chan amqp.Confirmation) chan amqp.Confirmation {
	s.confirmReceiver = receiver
	return receiver
}

func (s *fakeAMQPSession) NotifyReturn(receiver chan amqp.Return) chan amqp.Return {
	s.returnReceiver = receiver
	return receiver
}

func (s *fakeAMQPSession) PublishWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.publishCalls = append(s.publishCalls, publishCall{
		exchange:  exchange,
		key:       key,
		mandatory: mandatory,
		immediate: immediate,
		message:   msg,
	})
	if s.onPublish != nil {
		s.onPublish()
	}

	return s.publishErr
}

func (s *fakeAMQPSession) Close() error {
	s.closed = true
	return nil
}

type exchangeDeclareCall struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	args       amqp.Table
}

type queueDeclareCall struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	args       amqp.Table
}

type queueBindCall struct {
	name     string
	key      string
	exchange string
	noWait   bool
	args     amqp.Table
}

type publishCall struct {
	exchange  string
	key       string
	mandatory bool
	immediate bool
	message   amqp.Publishing
}
