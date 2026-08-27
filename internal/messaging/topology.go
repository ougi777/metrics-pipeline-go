package messaging

import amqp "github.com/rabbitmq/amqp091-go"

const (
	IngestExchange       = "metrics.exchange"
	IngestExchangeKind   = "direct"
	IngestQueue          = "metrics.ingest.v1"
	IngestRoutingKey     = "metrics.ingest"
	DeadLetterExchange   = "metrics.dlx"
	DeadLetterQueue      = "metrics.ingest.dlq"
	DeadLetterRoutingKey = "metrics.ingest.dlq"
)

type Topology struct {
	IngestExchange       string
	IngestExchangeKind   string
	IngestQueue          string
	IngestRoutingKey     string
	DeadLetterExchange   string
	DeadLetterQueue      string
	DeadLetterRoutingKey string
}

func DefaultTopology() Topology {
	return Topology{
		IngestExchange:       IngestExchange,
		IngestExchangeKind:   IngestExchangeKind,
		IngestQueue:          IngestQueue,
		IngestRoutingKey:     IngestRoutingKey,
		DeadLetterExchange:   DeadLetterExchange,
		DeadLetterQueue:      DeadLetterQueue,
		DeadLetterRoutingKey: DeadLetterRoutingKey,
	}
}

func declareTopology(session amqpSession, topology Topology) error {
	if topology == (Topology{}) {
		topology = DefaultTopology()
	}

	if err := session.ExchangeDeclare(topology.DeadLetterExchange, IngestExchangeKind, true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := session.QueueDeclare(topology.DeadLetterQueue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := session.QueueBind(topology.DeadLetterQueue, topology.DeadLetterRoutingKey, topology.DeadLetterExchange, false, nil); err != nil {
		return err
	}
	if err := session.ExchangeDeclare(topology.IngestExchange, topology.IngestExchangeKind, true, false, false, false, nil); err != nil {
		return err
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    topology.DeadLetterExchange,
		"x-dead-letter-routing-key": topology.DeadLetterRoutingKey,
	}
	if _, err := session.QueueDeclare(topology.IngestQueue, true, false, false, false, args); err != nil {
		return err
	}
	return session.QueueBind(topology.IngestQueue, topology.IngestRoutingKey, topology.IngestExchange, false, nil)
}
