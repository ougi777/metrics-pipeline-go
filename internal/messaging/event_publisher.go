package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

const metricEventSchemaVersion = 1

type MetricEventPublisherConfig struct {
	URL            string
	WriteTimeout   time.Duration
	ConfirmTimeout time.Duration
	Topology       Topology
}

type RabbitMQMetricEventPublisher struct {
	config  MetricEventPublisherConfig
	mu      sync.Mutex
	session publisherSession
	closed  bool
}

func NewRabbitMQMetricEventPublisher(config MetricEventPublisherConfig) (*RabbitMQMetricEventPublisher, error) {
	if config.URL == "" {
		return nil, errors.New("AMQP_URL must not be empty")
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = defaultAMQPWriteTimeout
	}
	if config.ConfirmTimeout <= 0 {
		config.ConfirmTimeout = 5 * time.Second
	}
	if config.Topology == (Topology{}) {
		config.Topology = DefaultTopology()
	}
	return &RabbitMQMetricEventPublisher{config: config}, nil
}

func (p *RabbitMQMetricEventPublisher) Publish(ctx context.Context, event domain.OutboxEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPublisherClosed
	}
	if p.session.client == nil {
		if err := p.openSession(ctx); err != nil {
			return err
		}
	}
	confirmCtx, cancel := context.WithTimeout(ctx, p.config.ConfirmTimeout)
	defer cancel()
	publishing := amqp.Publishing{
		Headers: amqp.Table{
			"schema_version": int32(metricEventSchemaVersion),
			"task_id":        event.TaskID,
			"event_seq":      event.EventSeq,
		},
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		MessageId:     fmt.Sprintf("%s:%d", event.TaskID, event.EventSeq),
		CorrelationId: fmt.Sprintf("%s:%d", event.TaskID, event.EventSeq),
		Body:          event.Payload,
	}
	if err := p.session.client.PublishWithContext(confirmCtx, p.config.Topology.RealtimeExchange, "", false, false, publishing); err != nil {
		p.resetSession()
		return err
	}
	for {
		select {
		case returned, ok := <-p.session.returns:
			if !ok {
				p.resetSession()
				return ErrConfirmClosed
			}
			p.resetSession()
			return unroutableError(returned)
		case confirmation, ok := <-p.session.confirms:
			if !ok {
				p.resetSession()
				return ErrConfirmClosed
			}
			if !confirmation.Ack {
				p.resetSession()
				return ErrBrokerNack
			}
			return nil
		case <-confirmCtx.Done():
			p.resetSession()
			if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ctx.Err()
			}
			return ErrConfirmTimeout
		}
	}
}

func (p *RabbitMQMetricEventPublisher) openSession(ctx context.Context) error {
	session, err := dialAMQPSession(ctx, p.config.URL, p.config.WriteTimeout)
	if err != nil {
		return fmt.Errorf("open realtime publisher session: %w", err)
	}
	if err := declareRealtimeTopology(session, p.config.Topology); err != nil {
		_ = session.Close()
		return fmt.Errorf("declare realtime topology: %w", err)
	}
	if err := session.Confirm(false); err != nil {
		_ = session.Close()
		return fmt.Errorf("enable realtime publisher confirms: %w", err)
	}
	p.session = publisherSession{
		client:   session,
		confirms: session.NotifyPublish(make(chan amqp.Confirmation, 1)),
		returns:  session.NotifyReturn(make(chan amqp.Return, 1)),
	}
	return nil
}

func (p *RabbitMQMetricEventPublisher) resetSession() {
	closePublisherSession(p.session)
	p.session = publisherSession{}
}

func (p *RabbitMQMetricEventPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		p.resetSession()
	}
	return nil
}

var _ interface {
	Publish(context.Context, domain.OutboxEvent) error
	Close() error
} = (*RabbitMQMetricEventPublisher)(nil)
