package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultConsumerBatchMax          = 500
	defaultConsumerFlushInterval     = 100 * time.Millisecond
	defaultConsumerPrefetch          = 500
	defaultConsumerFailureBackoff    = 100 * time.Millisecond
	defaultConsumerFailureMaxBackoff = 30 * time.Second
	defaultConsumerReconnectBackoff  = 100 * time.Millisecond
	defaultConsumerReconnectMaxDelay = 5 * time.Second
	defaultConsumerShutdownTimeout   = 5 * time.Second
)

var (
	ErrDeliveryStreamClosed = errors.New("delivery stream closed")
)

// MetricPointSink 持久化一次 flush 的全部指标点。
// 返回 nil 表示该批次已经安全提交，可以推进对应 delivery 的确认状态。
type MetricPointSink interface {
	Flush(context.Context, []domain.MetricPoint) error
}

type MetricPointSinkFunc func(context.Context, []domain.MetricPoint) error

func (f MetricPointSinkFunc) Flush(ctx context.Context, points []domain.MetricPoint) error {
	return f(ctx, points)
}

type ConsumerConfig struct {
	URL                 string
	Prefetch            int
	BatchMax            int
	FlushInterval       time.Duration
	FailureBackoff      time.Duration
	FailureMaxBackoff   time.Duration
	ReconnectBackoff    time.Duration
	ReconnectMaxBackoff time.Duration
	ShutdownTimeout     time.Duration
	Topology            Topology
	ConsumerTag         string
	sessionFactory      consumerSessionFactory
	sleep               func(context.Context, time.Duration) error
	jitter              func(time.Duration) time.Duration
}

type RabbitMQMetricConsumer struct {
	config ConsumerConfig
	sink   MetricPointSink
	logger *slog.Logger
}

type consumerSession interface {
	topologySession
	Qos(prefetchCount int, prefetchSize int, global bool) error
	Consume(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Close() error
}

type consumerSessionFactory func(context.Context, string) (consumerSession, error)

type deliveryState struct {
	delivery      amqp.Delivery
	totalRows     int
	committedRows int
	terminal      bool
}

type bufferedPoint struct {
	point    domain.MetricPoint
	delivery *deliveryState
}

func NewRabbitMQMetricConsumer(config ConsumerConfig, sink MetricPointSink, logger *slog.Logger) (*RabbitMQMetricConsumer, error) {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	if sink == nil {
		return nil, errors.New("metric point sink is required")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &RabbitMQMetricConsumer{config: config, sink: sink, logger: logger}, nil
}

// Run 持续建立消费会话；Channel 断开后会重建拓扑、QoS 和手动确认 consumer。
func (c *RabbitMQMetricConsumer) Run(ctx context.Context) error {
	backoff := c.config.ReconnectBackoff
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		session, deliveries, err := c.openSession(ctx)
		if err == nil {
			backoff = c.config.ReconnectBackoff
			err = c.consumeSession(ctx, deliveries)
			_ = session.Close()
			if ctx.Err() != nil {
				return nil
			}
		}

		c.logger.Warn("rabbitmq consumer session interrupted",
			slog.String("error_class", consumerErrorClass(err)),
			slog.Any("error", err),
			slog.Duration("reconnect_backoff", backoff),
		)
		if err := c.wait(ctx, c.config.jitter(backoff)); err != nil {
			return nil
		}
		backoff = min(backoff*2, c.config.ReconnectMaxBackoff)
	}
}

func (c *RabbitMQMetricConsumer) openSession(ctx context.Context) (consumerSession, <-chan amqp.Delivery, error) {
	session, err := c.config.sessionFactory(ctx, c.config.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("open consumer session: %w", err)
	}
	closeWithError := func(err error) (consumerSession, <-chan amqp.Delivery, error) {
		_ = session.Close()
		return nil, nil, err
	}
	if err := declareTopology(session, c.config.Topology); err != nil {
		return closeWithError(fmt.Errorf("declare topology: %w", err))
	}
	if err := session.Qos(c.config.Prefetch, 0, false); err != nil {
		return closeWithError(fmt.Errorf("configure qos: %w", err))
	}
	deliveries, err := session.Consume(c.config.Topology.IngestQueue, c.config.ConsumerTag, false, false, false, false, nil)
	if err != nil {
		return closeWithError(fmt.Errorf("start manual-ack consumer: %w", err))
	}

	return session, deliveries, nil
}

func (c *RabbitMQMetricConsumer) consumeSession(ctx context.Context, deliveries <-chan amqp.Delivery) error {
	buffer := make([]bufferedPoint, 0, c.config.BatchMax)
	timer := time.NewTimer(c.config.FlushInterval)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var timerC <-chan time.Time
	failureBackoff := c.config.FailureBackoff

	startTimer := func() {
		if timerC == nil && len(buffer) > 0 {
			timer.Reset(c.config.FlushInterval)
			timerC = timer.C
		}
	}
	stopTimer := func() {
		if timerC == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
	}
	flush := func(flushCtx context.Context) error {
		stopTimer()
		if len(buffer) == 0 {
			return nil
		}
		batch := buffer
		buffer = make([]bufferedPoint, 0, c.config.BatchMax)
		failed, err := c.flush(flushCtx, batch, failureBackoff)
		if failed {
			failureBackoff = min(failureBackoff*2, c.config.FailureMaxBackoff)
		} else if err == nil {
			failureBackoff = c.config.FailureBackoff
		}
		return err
	}

	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				return ErrDeliveryStreamClosed
			}
			message, err := validateDelivery(delivery)
			if err != nil {
				c.logProtocolError(ctx, delivery, err)
				if nackErr := delivery.Nack(false, false); nackErr != nil {
					return fmt.Errorf("dead-letter delivery %d: %w", delivery.DeliveryTag, nackErr)
				}
				continue
			}

			points := ExpandMetricPoints(message)
			state := &deliveryState{
				delivery:  delivery,
				totalRows: len(points),
			}
			for _, point := range points {
				if state.terminal {
					break
				}
				buffer = append(buffer, bufferedPoint{point: point, delivery: state})
				startTimer()
				if len(buffer) == c.config.BatchMax {
					if err := flush(ctx); err != nil {
						return err
					}
				}
			}
		case <-timerC:
			timerC = nil
			if err := flush(ctx); err != nil {
				return err
			}
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), c.config.ShutdownTimeout)
			err := flush(shutdownCtx)
			cancel()
			if err != nil {
				return err
			}
			return ctx.Err()
		}
	}
}

func (c *RabbitMQMetricConsumer) flush(ctx context.Context, batch []bufferedPoint, failureBackoff time.Duration) (bool, error) {
	points := make([]domain.MetricPoint, len(batch))
	for index := range batch {
		points[index] = batch[index].point
	}
	started := time.Now()
	err := c.sink.Flush(ctx, points)
	if err != nil {
		if waitErr := c.wait(ctx, c.config.jitter(failureBackoff)); waitErr != nil && ctx.Err() != nil {
			return true, waitErr
		}
		states := uniqueDeliveryStates(batch)
		for _, state := range states {
			state.terminal = true
			if nackErr := state.delivery.Nack(false, true); nackErr != nil {
				return true, fmt.Errorf("requeue delivery %d after flush failure: %w", state.delivery.DeliveryTag, nackErr)
			}
		}
		c.logger.Warn("metric point flush failed",
			slog.Int("flush_rows", len(points)),
			slog.Int64("duration_ms", time.Since(started).Milliseconds()),
			slog.Int("deliveries", len(states)),
			slog.Duration("failure_backoff", failureBackoff),
			slog.String("error_class", "flush_retryable"),
			slog.Any("error", err),
		)
		return true, nil
	}

	for state, rows := range committedRowsByDelivery(batch) {
		if state.terminal {
			continue
		}
		state.committedRows += rows
		if state.committedRows == state.totalRows {
			if err := state.delivery.Ack(false); err != nil {
				return false, fmt.Errorf("ack delivery %d: %w", state.delivery.DeliveryTag, err)
			}
			state.terminal = true
		}
	}
	c.logger.Debug("metric point flush completed",
		slog.Int("flush_rows", len(points)),
		slog.Int64("duration_ms", time.Since(started).Milliseconds()),
		slog.String("error_class", "none"),
	)

	return false, nil
}

func validateDelivery(delivery amqp.Delivery) (IngestMessage, error) {
	if !strings.EqualFold(delivery.ContentType, "application/json") {
		return IngestMessage{}, fmt.Errorf("content_type must be application/json, got %q", delivery.ContentType)
	}
	message, err := DecodeIngestMessage(delivery.Body)
	if err != nil {
		return IngestMessage{}, err
	}
	if delivery.MessageId == "" || delivery.MessageId != message.MessageID {
		return IngestMessage{}, errors.New("message_id property is missing or does not match body")
	}
	if delivery.CorrelationId == "" || delivery.CorrelationId != message.CorrelationID {
		return IngestMessage{}, errors.New("correlation_id property is missing or does not match body")
	}
	if value, ok := integerHeader(delivery.Headers["schema_version"]); !ok || value != int64(IngestSchemaVersion) {
		return IngestMessage{}, errors.New("schema_version header is missing or invalid")
	}
	if value, ok := delivery.Headers["message_id"].(string); !ok || value != message.MessageID {
		return IngestMessage{}, errors.New("message_id header is missing or does not match body")
	}
	if value, ok := delivery.Headers["correlation_id"].(string); !ok || value != message.CorrelationID {
		return IngestMessage{}, errors.New("correlation_id header is missing or does not match body")
	}

	return message, nil
}

func integerHeader(value any) (int64, bool) {
	switch value := value.(type) {
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case int:
		return int64(value), true
	default:
		return 0, false
	}
}

func uniqueDeliveryStates(batch []bufferedPoint) []*deliveryState {
	seen := make(map[*deliveryState]struct{})
	states := make([]*deliveryState, 0)
	for _, item := range batch {
		if _, ok := seen[item.delivery]; ok {
			continue
		}
		seen[item.delivery] = struct{}{}
		states = append(states, item.delivery)
	}
	return states
}

func committedRowsByDelivery(batch []bufferedPoint) map[*deliveryState]int {
	rows := make(map[*deliveryState]int)
	for _, item := range batch {
		rows[item.delivery]++
	}
	return rows
}

func (c *RabbitMQMetricConsumer) logProtocolError(ctx context.Context, delivery amqp.Delivery, err error) {
	c.logger.ErrorContext(ctx, "invalid ingest message dead-lettered",
		slog.String("message_id", delivery.MessageId),
		slog.Uint64("delivery_tag", delivery.DeliveryTag),
		slog.Bool("redelivered", delivery.Redelivered),
		slog.String("error_class", "message_protocol"),
		slog.Any("error", err),
	)
}

func (c *RabbitMQMetricConsumer) wait(ctx context.Context, duration time.Duration) error {
	if c.config.sleep != nil {
		return c.config.sleep(ctx, duration)
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c ConsumerConfig) withDefaults() ConsumerConfig {
	if c.Prefetch == 0 {
		c.Prefetch = defaultConsumerPrefetch
	}
	if c.BatchMax == 0 {
		c.BatchMax = defaultConsumerBatchMax
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = defaultConsumerFlushInterval
	}
	if c.FailureBackoff == 0 {
		c.FailureBackoff = defaultConsumerFailureBackoff
	}
	if c.FailureMaxBackoff == 0 {
		c.FailureMaxBackoff = defaultConsumerFailureMaxBackoff
	}
	if c.ReconnectBackoff == 0 {
		c.ReconnectBackoff = defaultConsumerReconnectBackoff
	}
	if c.ReconnectMaxBackoff == 0 {
		c.ReconnectMaxBackoff = defaultConsumerReconnectMaxDelay
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = defaultConsumerShutdownTimeout
	}
	if c.Topology == (Topology{}) {
		c.Topology = DefaultTopology()
	}
	if c.ConsumerTag == "" {
		c.ConsumerTag = "metrics-worker"
	}
	if c.sessionFactory == nil {
		c.sessionFactory = dialConsumerSession
	}
	if c.jitter == nil {
		c.jitter = jitterDuration
	}
	return c
}

func (c ConsumerConfig) validate() error {
	if c.URL == "" {
		return errors.New("AMQP_URL must not be empty")
	}
	if c.Prefetch <= 0 || c.BatchMax <= 0 {
		return errors.New("consumer prefetch and batch max must be greater than zero")
	}
	if c.FlushInterval <= 0 || c.FailureBackoff <= 0 || c.ReconnectBackoff <= 0 || c.ShutdownTimeout <= 0 {
		return errors.New("consumer durations must be greater than zero")
	}
	if c.FailureMaxBackoff < c.FailureBackoff {
		return errors.New("consumer failure max backoff must be greater than or equal to initial backoff")
	}
	if c.ReconnectMaxBackoff < c.ReconnectBackoff {
		return errors.New("consumer reconnect max backoff must be greater than or equal to initial backoff")
	}
	return nil
}

func dialConsumerSession(ctx context.Context, url string) (consumerSession, error) {
	session, err := dialAMQPSession(ctx, url, defaultAMQPWriteTimeout)
	if err != nil {
		return nil, err
	}
	consumer, ok := session.(consumerSession)
	if !ok {
		_ = session.Close()
		return nil, errors.New("AMQP session does not support consuming")
	}
	return consumer, nil
}

func jitterDuration(duration time.Duration) time.Duration {
	half := duration / 2
	if half <= 0 {
		return duration
	}
	return half + time.Duration(rand.Int64N(int64(duration-half)+1))
}

func consumerErrorClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, ErrDeliveryStreamClosed):
		return "delivery_stream_closed"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "context_deadline"
	default:
		return "amqp"
	}
}

func (s realAMQPSession) Qos(prefetchCount int, prefetchSize int, global bool) error {
	return s.channel.Qos(prefetchCount, prefetchSize, global)
}

func (s realAMQPSession) Consume(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return s.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}
