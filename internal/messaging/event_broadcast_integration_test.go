//go:build integration

package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRealtimePublisherFansOutToIndependentAPIQueues(t *testing.T) {
	url := integrationAMQPURL()
	topology := integrationTopology(t)
	topology.RealtimeExchange = fmt.Sprintf("metrics.test.realtime.%d", time.Now().UnixNano())
	topology.RealtimeExchangeKind = RealtimeExchangeKind

	connections := make([]*amqp.Connection, 2)
	channels := make([]*amqp.Channel, 2)
	deliveries := make([]<-chan amqp.Delivery, 2)
	for i := range connections {
		connection, err := amqp.Dial(url)
		if err != nil {
			t.Fatalf("dial API connection %d: %v", i, err)
		}
		connections[i] = connection
		channel, err := connection.Channel()
		if err != nil {
			t.Fatalf("open API channel %d: %v", i, err)
		}
		channels[i] = channel
		if err := declareRealtimeTopology(channel, topology); err != nil {
			t.Fatalf("declare realtime topology: %v", err)
		}
		queue, err := channel.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			t.Fatalf("declare API queue %d: %v", i, err)
		}
		if err := channel.QueueBind(queue.Name, "", topology.RealtimeExchange, false, nil); err != nil {
			t.Fatalf("bind API queue %d: %v", i, err)
		}
		deliveries[i], err = channel.Consume(queue.Name, "test-api-instance", false, true, false, false, nil)
		if err != nil {
			t.Fatalf("consume API queue %d: %v", i, err)
		}
	}
	t.Cleanup(func() {
		for _, channel := range channels {
			_ = channel.ExchangeDelete(topology.RealtimeExchange, false, false)
			_ = channel.Close()
		}
		for _, connection := range connections {
			_ = connection.Close()
		}
	})

	publisher, err := NewRabbitMQMetricEventPublisher(MetricEventPublisherConfig{URL: url, Topology: topology, ConfirmTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	payload, _ := json.Marshal(map[string]any{"points": []map[string]any{{"step": 3, "loss": 1.2}}})
	if err := publisher.Publish(context.Background(), domain.OutboxEvent{TaskID: "fanout-task", EventSeq: 9, Payload: payload}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	for i, stream := range deliveries {
		select {
		case delivery := <-stream:
			if delivery.Headers["task_id"] != "fanout-task" || delivery.Headers["event_seq"] != int64(9) {
				t.Fatalf("queue %d headers = %#v", i, delivery.Headers)
			}
			if string(delivery.Body) != string(payload) {
				t.Fatalf("queue %d body = %s, want %s", i, delivery.Body, payload)
			}
			if err := delivery.Ack(false); err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for queue %d", i)
		}
	}
}
