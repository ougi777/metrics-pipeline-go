package messaging

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestEventBridgeBacksOffAfterSinkFailure(t *testing.T) {
	firstAck := &recordingAcknowledger{}
	secondAck := &recordingAcknowledger{}
	deliveries := make(chan amqp.Delivery, 2)
	deliveries <- realtimeDelivery(firstAck, 1, "task-a")
	deliveries <- realtimeDelivery(secondAck, 1, "task-a")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &retryingEventSink{cancel: cancel}
	bridge, err := NewRabbitMQMetricEventBridge(EventBridgeConfig{
		URL:                   "amqp://test",
		InstanceID:            "api-1",
		SinkFailureBackoff:    20 * time.Millisecond,
		SinkFailureMaxBackoff: 40 * time.Millisecond,
	}, sink, nil)
	if err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	if err := bridge.consumeSession(ctx, nil, deliveries); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("consumeSession() error = %v", err)
	}

	sink.mu.Lock()
	times := append([]time.Time(nil), sink.times...)
	sink.mu.Unlock()
	if len(times) != 2 {
		t.Fatalf("sink calls = %d, want 2", len(times))
	}
	if elapsed := times[1].Sub(times[0]); elapsed < 15*time.Millisecond {
		t.Fatalf("retry delay = %v, want at least 15ms (started %v ago)", elapsed, time.Since(started))
	}
	if firstAck.nackCount() != 1 {
		t.Fatalf("first delivery nack count = %d, want 1", firstAck.nackCount())
	}
	if secondAck.ackCount() != 1 {
		t.Fatalf("second delivery ack count = %d, want 1", secondAck.ackCount())
	}
}

func realtimeDelivery(ack *recordingAcknowledger, tag uint64, taskID string) amqp.Delivery {
	return amqp.Delivery{
		Acknowledger: ack,
		DeliveryTag:  tag,
		Headers: map[string]any{
			"task_id":   taskID,
			"event_seq": int64(1),
		},
		Body: []byte(`{"points":[]}`),
	}
}

func (a *recordingAcknowledger) nackCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.nacks)
}

type retryingEventSink struct {
	mu     sync.Mutex
	times  []time.Time
	calls  int
	cancel context.CancelFunc
}

func (s *retryingEventSink) HandleMetricEvent(context.Context, domain.RealtimeEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.times = append(s.times, time.Now())
	if s.calls == 1 {
		return errors.New("sink unavailable")
	}
	s.cancel()
	return nil
}
