package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultAMQPWriteTimeout      = 5 * time.Second
	defaultAMQPConnectionTimeout = 30 * time.Second
	amqpLocale                   = "en_US"
)

var (
	ErrPublisherClosed = errors.New("publisher closed")
	ErrPublishFailed   = errors.New("publish failed")
	ErrUnroutable      = errors.New("message unroutable")
	ErrBrokerNack      = errors.New("broker nack")
	ErrConfirmTimeout  = errors.New("publisher confirm timeout")
	ErrConfirmClosed   = errors.New("publisher confirm channel closed")
)

type PublisherConfig struct {
	URL            string
	Publishers     int //publisher并发数量
	WriteTimeout   time.Duration
	ConfirmTimeout time.Duration
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Topology       Topology
	IDGenerator    IDGenerator
	Clock          func() time.Time
	sessionFactory amqpSessionFactory
	sleep          func(context.Context, time.Duration) error
}

type RabbitMQMetricBatchPublisher struct {
	config          PublisherConfig
	logger          *slog.Logger
	jobs            chan publishJob
	done            chan struct{}
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	once            sync.Once
	wg              sync.WaitGroup
}

type publishJob struct {
	ctx    context.Context
	batch  domain.MetricBatch
	result chan error
}

type publisherSession struct {
	client   amqpSession
	confirms <-chan amqp.Confirmation
	returns  <-chan amqp.Return
}

type amqpSessionFactory func(context.Context, string) (amqpSession, error)

type amqpSession interface {
	ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name string, key string, exchange string, noWait bool, args amqp.Table) error
	Confirm(noWait bool) error
	NotifyPublish(receiver chan amqp.Confirmation) chan amqp.Confirmation
	NotifyReturn(receiver chan amqp.Return) chan amqp.Return
	PublishWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error
	Close() error
}

func NewRabbitMQMetricBatchPublisher(ctx context.Context, config PublisherConfig, logger *slog.Logger) (*RabbitMQMetricBatchPublisher, error) {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	publisher := &RabbitMQMetricBatchPublisher{
		config:          config,
		logger:          logger,
		jobs:            make(chan publishJob, config.Publishers*16),
		done:            make(chan struct{}),
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
	}

	for index := 0; index < config.Publishers; index++ {
		session, err := publisher.openSession(ctx)
		if err != nil {
			_ = publisher.Close()
			return nil, fmt.Errorf("open publisher session %d: %w", index, err)
		}
		publisher.wg.Add(1)
		go publisher.runWorker(index, session)
	}

	return publisher, nil
}

func (p *RabbitMQMetricBatchPublisher) PublishMetricBatch(ctx context.Context, batch domain.MetricBatch) error {
	result := make(chan error, 1)
	job := publishJob{ctx: ctx, batch: batch, result: result}

	// 第一阶段：提交任务
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return ErrPublisherClosed
	case p.jobs <- job:
	}

	// 第二阶段：等待结果
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return ErrPublisherClosed
	case err := <-result:
		return err
	}
}

func (p *RabbitMQMetricBatchPublisher) Close() error {
	p.once.Do(func() {
		p.lifecycleCancel()
		close(p.done)
	})
	p.wg.Wait()

	return nil
}

func (p *RabbitMQMetricBatchPublisher) runWorker(workerID int, session publisherSession) {
	defer p.wg.Done()
	defer func() {
		closePublisherSession(session)
	}()

	for {
		select {
		case <-p.done:
			return
		case job := <-p.jobs:
			err := p.publishWithRetry(job.ctx, workerID, &session, job.batch)
			job.result <- err
		}
	}
}

func (p *RabbitMQMetricBatchPublisher) publishWithRetry(ctx context.Context, workerID int, session *publisherSession, batch domain.MetricBatch) error {
	message, err := NewIngestMessage(batch, p.config.IDGenerator)
	if err != nil {
		p.logPublishAttempt(ctx, slog.LevelError, workerID, batch.TaskID, "", 0, 0, time.Time{}, err)
		return err
	}
	publishing, err := MarshalIngestPublishing(message, p.config.Clock())
	if err != nil {
		p.logPublishAttempt(ctx, slog.LevelError, workerID, batch.TaskID, message.MessageID, 0, 0, time.Time{}, err)
		return err
	}

	var lastErr error
	for attempt := 1; attempt <= p.config.MaxAttempts; attempt++ {
		started := time.Now()
		deliveryTag := uint64(0)
		var attemptErr error
		if session.client == nil {
			nextSession, err := p.openSession(ctx)
			if err != nil {
				attemptErr = err
			} else {
				*session = nextSession
			}
		}

		if session.client != nil {
			deliveryTag, attemptErr = p.publishOnce(ctx, *session, publishing)
		}
		lastErr = attemptErr
		level := slog.LevelWarn
		if attemptErr == nil {
			level = slog.LevelDebug
		}
		p.logPublishAttempt(ctx, level, workerID, batch.TaskID, message.MessageID, attempt, deliveryTag, started, attemptErr)
		if attemptErr == nil {
			return nil
		}
		closePublisherSession(*session)
		*session = publisherSession{}

		if attempt == p.config.MaxAttempts {
			break
		}
		if err := p.waitBackoff(ctx, p.backoff(attempt)); err != nil {
			return err
		}
	}

	if lastErr == nil {
		return fmt.Errorf("%w after %d attempts", ErrPublishFailed, p.config.MaxAttempts)
	}

	return fmt.Errorf("%w after %d attempts: %w", ErrPublishFailed, p.config.MaxAttempts, lastErr)
}

func (p *RabbitMQMetricBatchPublisher) publishOnce(ctx context.Context, session publisherSession, publishing amqp.Publishing) (uint64, error) {
	confirmCtx, cancel := context.WithTimeout(ctx, p.config.ConfirmTimeout)
	defer cancel()

	topology := p.config.Topology
	if topology == (Topology{}) {
		topology = DefaultTopology()
	}

	if err := confirmCtx.Err(); err != nil {
		return 0, err
	}
	if err := session.client.PublishWithContext(confirmCtx, topology.IngestExchange, topology.IngestRoutingKey, true, false, publishing); err != nil {
		return 0, err
	}

	for {
		select {
		case returned, ok := <-session.returns:
			if !ok {
				return 0, ErrConfirmClosed
			}
			return 0, unroutableError(returned)
		case confirmation, ok := <-session.confirms:
			if !ok {
				return 0, ErrConfirmClosed
			}
			// RabbitMQ notifies mandatory returns before acknowledging the publish.
			select {
			case returned, ok := <-session.returns:
				if !ok {
					return confirmation.DeliveryTag, ErrConfirmClosed
				}
				return confirmation.DeliveryTag, unroutableError(returned)
			default:
			}
			if !confirmation.Ack {
				return confirmation.DeliveryTag, ErrBrokerNack
			}
			return confirmation.DeliveryTag, nil
		case <-confirmCtx.Done():
			if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return 0, ctx.Err()
			}
			return 0, ErrConfirmTimeout
		case <-p.done:
			return 0, ErrPublisherClosed
		}
	}
}

func (p *RabbitMQMetricBatchPublisher) logPublishAttempt(
	ctx context.Context,
	level slog.Level,
	workerID int,
	taskID string,
	messageID string,
	attempt int,
	deliveryTag uint64,
	started time.Time,
	err error,
) {
	duration := time.Duration(0)
	if !started.IsZero() {
		duration = time.Since(started)
	}
	p.logger.Log(ctx, level, "metric batch publish attempt completed",
		slog.Int("worker_id", workerID),
		slog.String("task_id", taskID),
		slog.String("message_id", messageID),
		slog.Uint64("delivery_tag", deliveryTag),
		slog.Int("attempt", attempt),
		slog.Int64("duration_ms", duration.Milliseconds()),
		slog.String("error_class", publishErrorClass(err)),
		slog.Any("error", err),
	)
}

func publishErrorClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, ErrUnroutable):
		return "unroutable"
	case errors.Is(err, ErrBrokerNack):
		return "broker_nack"
	case errors.Is(err, ErrConfirmTimeout):
		return "confirm_timeout"
	case errors.Is(err, ErrConfirmClosed):
		return "confirm_closed"
	case errors.Is(err, ErrPublisherClosed):
		return "publisher_closed"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "context_deadline"
	default:
		return "amqp"
	}
}

func unroutableError(returned amqp.Return) error {
	return fmt.Errorf("%w: exchange=%s routing_key=%s reply=%d %s", ErrUnroutable, returned.Exchange, returned.RoutingKey, returned.ReplyCode, returned.ReplyText)
}

func (p *RabbitMQMetricBatchPublisher) openSession(ctx context.Context) (publisherSession, error) {
	dialCtx, cancel := context.WithCancel(ctx)
	stopLifecycleWatch := context.AfterFunc(p.lifecycleCtx, cancel)
	defer func() {
		stopLifecycleWatch()
		cancel()
	}()

	if err := dialCtx.Err(); err != nil {
		return publisherSession{}, err
	}

	client, err := p.config.sessionFactory(dialCtx, p.config.URL)
	if err != nil {
		return publisherSession{}, err
	}
	if err := declareTopology(client, p.config.Topology); err != nil {
		_ = client.Close()
		return publisherSession{}, err
	}
	if err := client.Confirm(false); err != nil {
		_ = client.Close()
		return publisherSession{}, err
	}

	return publisherSession{
		client:   client,
		confirms: client.NotifyPublish(make(chan amqp.Confirmation, 1)),
		returns:  client.NotifyReturn(make(chan amqp.Return, 1)),
	}, nil
}

func (p *RabbitMQMetricBatchPublisher) waitBackoff(ctx context.Context, duration time.Duration) error {
	if p.config.sleep != nil {
		return p.config.sleep(ctx, duration)
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return ErrPublisherClosed
	case <-timer.C:
		return nil
	}
}

func (p *RabbitMQMetricBatchPublisher) backoff(attempt int) time.Duration {
	duration := p.config.InitialBackoff
	for count := 1; count < attempt; count++ {
		duration *= 2
		if duration >= p.config.MaxBackoff {
			return p.config.MaxBackoff
		}
	}

	return duration
}

func (c PublisherConfig) withDefaults() PublisherConfig {
	if c.Publishers == 0 {
		c.Publishers = 1
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultAMQPWriteTimeout
	}
	if c.ConfirmTimeout == 0 {
		c.ConfirmTimeout = 5 * time.Second
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 3
	}
	if c.InitialBackoff == 0 {
		c.InitialBackoff = 100 * time.Millisecond
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = time.Second
	}
	if c.Topology == (Topology{}) {
		c.Topology = DefaultTopology()
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	if c.sessionFactory == nil {
		writeTimeout := c.WriteTimeout
		c.sessionFactory = func(ctx context.Context, url string) (amqpSession, error) {
			return dialAMQPSession(ctx, url, writeTimeout)
		}
	}

	return c
}

func (c PublisherConfig) validate() error {
	if c.URL == "" {
		return errors.New("AMQP_URL must not be empty")
	}
	if c.Publishers <= 0 {
		return errors.New("AMQP_PUBLISHERS must be greater than zero")
	}
	if c.WriteTimeout <= 0 {
		return errors.New("AMQP_WRITE_TIMEOUT must be greater than zero")
	}
	if c.ConfirmTimeout <= 0 {
		return errors.New("AMQP_CONFIRM_TIMEOUT must be greater than zero")
	}
	if c.MaxAttempts <= 0 {
		return errors.New("AMQP_PUBLISH_MAX_ATTEMPTS must be greater than zero")
	}
	if c.InitialBackoff <= 0 {
		return errors.New("AMQP_PUBLISH_INITIAL_BACKOFF must be greater than zero")
	}
	if c.MaxBackoff < c.InitialBackoff {
		return errors.New("AMQP_PUBLISH_MAX_BACKOFF must be greater than or equal to AMQP_PUBLISH_INITIAL_BACKOFF")
	}

	return nil
}

func closePublisherSession(session publisherSession) {
	if session.client != nil {
		_ = session.client.Close()
	}
}

func dialAMQPSession(ctx context.Context, url string, writeTimeout time.Duration) (amqpSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	dialer := net.Dialer{Timeout: defaultAMQPConnectionTimeout}
	connection, err := amqp.DialConfig(url, amqp.Config{
		Locale: amqpLocale,
		Dial: func(network, address string) (net.Conn, error) {
			connection, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}

			handshakeDeadline := time.Now().Add(defaultAMQPConnectionTimeout)
			if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(handshakeDeadline) {
				handshakeDeadline = contextDeadline
			}
			if err := connection.SetDeadline(handshakeDeadline); err != nil {
				_ = connection.Close()
				return nil, err
			}

			return &writeDeadlineConn{Conn: connection, timeout: writeTimeout}, nil
		},
	})
	if err != nil {
		return nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, err
	}

	return realAMQPSession{connection: connection, channel: channel}, nil
}

type writeDeadlineConn struct {
	net.Conn
	timeout time.Duration
}

func (c *writeDeadlineConn) Write(data []byte) (int, error) {
	if err := c.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}

	return c.Conn.Write(data)
}

type realAMQPSession struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func (s realAMQPSession) ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error {
	return s.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (s realAMQPSession) QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return s.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (s realAMQPSession) QueueBind(name string, key string, exchange string, noWait bool, args amqp.Table) error {
	return s.channel.QueueBind(name, key, exchange, noWait, args)
}

func (s realAMQPSession) Confirm(noWait bool) error {
	return s.channel.Confirm(noWait)
}

func (s realAMQPSession) NotifyPublish(receiver chan amqp.Confirmation) chan amqp.Confirmation {
	return s.channel.NotifyPublish(receiver)
}

func (s realAMQPSession) NotifyReturn(receiver chan amqp.Return) chan amqp.Return {
	return s.channel.NotifyReturn(receiver)
}

func (s realAMQPSession) PublishWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error {
	return s.channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (s realAMQPSession) Close() error {
	channelErr := s.channel.Close()
	connectionErr := s.connection.Close()
	if channelErr != nil {
		return channelErr
	}

	return connectionErr
}
