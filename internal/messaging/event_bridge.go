package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

type EventSink interface {
	HandleMetricEvent(context.Context, domain.RealtimeEvent) error
}

type EventSinkFunc func(context.Context, domain.RealtimeEvent) error

func (f EventSinkFunc) HandleMetricEvent(ctx context.Context, event domain.RealtimeEvent) error {
	return f(ctx, event)
}

type EventBridgeConfig struct {
	URL                   string
	InstanceID            string
	Topology              Topology
	SinkFailureBackoff    time.Duration
	SinkFailureMaxBackoff time.Duration
	ReconnectBackoff      time.Duration
	ReconnectMaxBackoff   time.Duration
}

type RabbitMQMetricEventBridge struct {
	config EventBridgeConfig
	sink   EventSink
	logger *slog.Logger
	mu     sync.Mutex
	last   map[string]int64
}

func NewRabbitMQMetricEventBridge(config EventBridgeConfig, sink EventSink, logger *slog.Logger) (*RabbitMQMetricEventBridge, error) {
	if config.URL == "" {
		return nil, errors.New("AMQP_URL must not be empty")
	}
	if config.InstanceID == "" {
		return nil, errors.New("instance id is required")
	}
	if sink == nil {
		return nil, errors.New("event sink is required")
	}
	if config.ReconnectBackoff == 0 {
		config.ReconnectBackoff = 100 * time.Millisecond
	}
	if config.ReconnectMaxBackoff == 0 {
		config.ReconnectMaxBackoff = 5 * time.Second
	}
	if config.SinkFailureBackoff == 0 {
		config.SinkFailureBackoff = 100 * time.Millisecond
	}
	if config.SinkFailureMaxBackoff == 0 {
		config.SinkFailureMaxBackoff = 5 * time.Second
	}
	if config.SinkFailureBackoff <= 0 || config.SinkFailureMaxBackoff < config.SinkFailureBackoff || config.ReconnectBackoff <= 0 || config.ReconnectMaxBackoff < config.ReconnectBackoff {
		return nil, errors.New("invalid event bridge backoff configuration")
	}
	if config.Topology == (Topology{}) {
		config.Topology = DefaultTopology()
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &RabbitMQMetricEventBridge{config: config, sink: sink, logger: logger, last: make(map[string]int64)}, nil
}

func (b *RabbitMQMetricEventBridge) Run(ctx context.Context) error {
	backoff := b.config.ReconnectBackoff
	for ctx.Err() == nil {
		session, deliveries, err := b.openSession(ctx)
		if err == nil {
			backoff = b.config.ReconnectBackoff
			err = b.consumeSession(ctx, session, deliveries)
			_ = session.Close()
			if ctx.Err() != nil {
				return nil
			}
		}
		b.logger.Warn("realtime event bridge session interrupted", slog.Any("error", err), slog.Duration("reconnect_backoff", backoff))
		if !waitContext(ctx, backoff) {
			return nil
		}
		backoff = minDuration(backoff*2, b.config.ReconnectMaxBackoff)
	}
	return nil
}

func (b *RabbitMQMetricEventBridge) openSession(ctx context.Context) (consumerSession, <-chan amqp.Delivery, error) {
	session, err := dialConsumerSession(ctx, b.config.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("open realtime event bridge session: %w", err)
	}
	closeWithError := func(err error) (consumerSession, <-chan amqp.Delivery, error) {
		_ = session.Close()
		return nil, nil, err
	}
	if err := declareRealtimeTopology(session, b.config.Topology); err != nil {
		return closeWithError(fmt.Errorf("declare realtime exchange: %w", err))
	}
	queue, err := session.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return closeWithError(fmt.Errorf("declare realtime instance queue: %w", err))
	}
	if err := session.QueueBind(queue.Name, "", b.config.Topology.RealtimeExchange, false, nil); err != nil {
		return closeWithError(fmt.Errorf("bind realtime instance queue: %w", err))
	}
	deliveries, err := session.Consume(queue.Name, "metrics-events-"+b.config.InstanceID, false, true, false, false, nil)
	if err != nil {
		return closeWithError(fmt.Errorf("consume realtime instance queue: %w", err))
	}
	return session, deliveries, nil
}

func (b *RabbitMQMetricEventBridge) consumeSession(ctx context.Context, session consumerSession, deliveries <-chan amqp.Delivery) error {
	sinkBackoff := b.config.SinkFailureBackoff
	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				return ErrDeliveryStreamClosed
			}
			event, err := decodeRealtimeEvent(delivery)
			if err != nil {
				b.logger.Error("invalid realtime event acknowledged", slog.Any("error", err))
				if ackErr := delivery.Ack(false); ackErr != nil {
					return ackErr
				}
				continue
			}
			if b.seen(event) {
				if err := delivery.Ack(false); err != nil {
					return err
				}
				continue
			}

			//广播到所有该api实例连接的SSE客户端
			if err := b.sink.HandleMetricEvent(ctx, event); err != nil {
				if nackErr := delivery.Nack(false, true); nackErr != nil {
					return errors.Join(err, nackErr)
				}
				if !waitContext(ctx, sinkBackoff) {
					return ctx.Err()
				}
				sinkBackoff = minDuration(sinkBackoff*2, b.config.SinkFailureMaxBackoff)
				continue
			}
			sinkBackoff = b.config.SinkFailureBackoff
			b.remember(event)
			if err := delivery.Ack(false); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func decodeRealtimeEvent(delivery amqp.Delivery) (domain.RealtimeEvent, error) {
	taskID, ok := delivery.Headers["task_id"].(string)
	if !ok || taskID == "" {
		return domain.RealtimeEvent{}, errors.New("task_id header is required")
	}
	seq, err := headerInt64(delivery.Headers["event_seq"])
	if err != nil || seq <= 0 {
		return domain.RealtimeEvent{}, errors.New("event_seq header is invalid")
	}
	if !json.Valid(delivery.Body) {
		return domain.RealtimeEvent{}, errors.New("event payload is invalid JSON")
	}
	return domain.RealtimeEvent{TaskID: taskID, EventSeq: seq, Payload: append(json.RawMessage(nil), delivery.Body...)}, nil
}

func headerInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, errors.New("unsupported header type")
	}
}

func (b *RabbitMQMetricEventBridge) seen(event domain.RealtimeEvent) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return event.EventSeq <= b.last[event.TaskID]
}

func (b *RabbitMQMetricEventBridge) remember(event domain.RealtimeEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if event.EventSeq > b.last[event.TaskID] {
		b.last[event.TaskID] = event.EventSeq
	}
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
