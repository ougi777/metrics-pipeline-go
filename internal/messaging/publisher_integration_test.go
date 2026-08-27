//go:build integration

package messaging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQPublisherReturnsUnroutableFromRealBroker(t *testing.T) {
	url := integrationAMQPURL()
	topology := integrationTopology(t)
	defer cleanupIntegrationTopology(t, url, topology)

	publisher := newIntegrationPublisher(t, PublisherConfig{
		URL:            url,
		Publishers:     1,
		WriteTimeout:   time.Second,
		ConfirmTimeout: 5 * time.Second,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		Topology:       topology,
	})
	defer closeTestPublisher(t, publisher)

	deleteIntegrationQueue(t, url, topology.IngestQueue)
	err := publisher.PublishMetricBatch(context.Background(), sampleMetricBatch())
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("PublishMetricBatch() error = %v, want unroutable", err)
	}
}

func TestRabbitMQPublisherReconnectsAfterRealConnectionLoss(t *testing.T) {
	url := integrationAMQPURL()
	topology := integrationTopology(t)
	defer cleanupIntegrationTopology(t, url, topology)

	var factoryCalls atomic.Int32
	factory := func(ctx context.Context, url string) (amqpSession, error) {
		session, err := dialAMQPSession(ctx, url, time.Second)
		if err != nil {
			return nil, err
		}
		if factoryCalls.Add(1) == 1 {
			return &failFirstRealPublishSession{amqpSession: session}, nil
		}
		return session, nil
	}
	publisher := newIntegrationPublisher(t, PublisherConfig{
		URL:            url,
		Publishers:     1,
		WriteTimeout:   time.Second,
		ConfirmTimeout: 5 * time.Second,
		MaxAttempts:    2,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Topology:       topology,
		sessionFactory: factory,
	})
	defer closeTestPublisher(t, publisher)

	if err := publisher.PublishMetricBatch(context.Background(), sampleMetricBatch()); err != nil {
		t.Fatalf("PublishMetricBatch() error = %v", err)
	}
	if got := factoryCalls.Load(); got != 2 {
		t.Fatalf("session factory calls = %d, want 2", got)
	}
}

func TestRabbitMQPublisherReconnectsAfterBrokerRestart(t *testing.T) {
	if os.Getenv("RUN_RABBITMQ_RESTART_TEST") != "1" {
		t.Skip("set RUN_RABBITMQ_RESTART_TEST=1 to run the disruptive broker restart test")
	}

	url := integrationAMQPURL()
	topology := integrationTopology(t)
	defer cleanupIntegrationTopology(t, url, topology)
	publisher := newIntegrationPublisher(t, PublisherConfig{
		URL:            url,
		Publishers:     1,
		WriteTimeout:   time.Second,
		ConfirmTimeout: 2 * time.Second,
		MaxAttempts:    60,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
		Topology:       topology,
	})
	defer closeTestPublisher(t, publisher)

	composeFile := filepath.Join("..", "..", "compose.yaml")
	runDockerCompose(t, composeFile, "stop", "rabbitmq")
	brokerStopped := true
	defer func() {
		if brokerStopped {
			runDockerCompose(t, composeFile, "start", "rabbitmq")
		}
	}()

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- publisher.PublishMetricBatch(context.Background(), sampleMetricBatch())
	}()
	time.Sleep(500 * time.Millisecond)
	runDockerCompose(t, composeFile, "start", "rabbitmq")
	brokerStopped = false

	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("PublishMetricBatch() after broker restart error = %v", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("PublishMetricBatch() did not recover after broker restart")
	}
}

type failFirstRealPublishSession struct {
	amqpSession
	once sync.Once
}

func (s *failFirstRealPublishSession) PublishWithContext(
	context.Context,
	string,
	string,
	bool,
	bool,
	amqp.Publishing,
) error {
	failed := false
	s.once.Do(func() {
		failed = true
		_ = s.amqpSession.Close()
	})
	if failed {
		return net.ErrClosed
	}
	return errors.New("unexpected repeated publish on closed session")
}

func newIntegrationPublisher(t *testing.T, config PublisherConfig) *RabbitMQMetricBatchPublisher {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	publisher, err := NewRabbitMQMetricBatchPublisher(context.Background(), config, logger)
	if err != nil {
		t.Fatalf("NewRabbitMQMetricBatchPublisher() error = %v", err)
	}
	return publisher
}

func integrationAMQPURL() string {
	if url := os.Getenv("AMQP_INTEGRATION_URL"); url != "" {
		return url
	}
	return "amqp://metrics:metrics@localhost:5672/"
}

func integrationTopology(t *testing.T) Topology {
	t.Helper()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	return Topology{
		IngestExchange:       "metrics.test.exchange." + suffix,
		IngestExchangeKind:   IngestExchangeKind,
		IngestQueue:          "metrics.test.ingest." + suffix,
		IngestRoutingKey:     "metrics.test.route." + suffix,
		DeadLetterExchange:   "metrics.test.dlx." + suffix,
		DeadLetterQueue:      "metrics.test.dlq." + suffix,
		DeadLetterRoutingKey: "metrics.test.dead." + suffix,
	}
}

func deleteIntegrationQueue(t *testing.T, url string, queue string) {
	t.Helper()

	connection, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("Dial(control connection) error = %v", err)
	}
	defer func() { _ = connection.Close() }()
	channel, err := connection.Channel()
	if err != nil {
		t.Fatalf("Channel(control connection) error = %v", err)
	}
	defer func() { _ = channel.Close() }()
	if _, err := channel.QueueDelete(queue, false, false, false); err != nil {
		t.Fatalf("QueueDelete(%s) error = %v", queue, err)
	}
}

func cleanupIntegrationTopology(t *testing.T, url string, topology Topology) {
	t.Helper()

	connection, err := amqp.Dial(url)
	if err != nil {
		t.Logf("cleanup Dial() error = %v", err)
		return
	}
	defer func() { _ = connection.Close() }()
	channel, err := connection.Channel()
	if err != nil {
		t.Logf("cleanup Channel() error = %v", err)
		return
	}
	defer func() { _ = channel.Close() }()
	_, _ = channel.QueueDelete(topology.IngestQueue, false, false, false)
	_, _ = channel.QueueDelete(topology.DeadLetterQueue, false, false, false)
	_ = channel.ExchangeDelete(topology.IngestExchange, false, false)
	_ = channel.ExchangeDelete(topology.DeadLetterExchange, false, false)
}

func runDockerCompose(t *testing.T, composeFile string, args ...string) {
	t.Helper()

	commandArgs := append([]string{"compose", "-f", composeFile}, args...)
	command := exec.Command("docker", commandArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v error = %v; output: %s", commandArgs, err, output)
	}
}
