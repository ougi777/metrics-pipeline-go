package domain

import "encoding/json"

// OutboxEvent is a committed task event awaiting realtime publication.
type OutboxEvent struct {
	ID         int64
	TaskID     string
	EventSeq   int64
	Payload    json.RawMessage
	ClaimToken string
}

// RealtimeEvent is an event received from the RabbitMQ realtime fanout.
type RealtimeEvent struct {
	TaskID   string
	EventSeq int64
	Payload  json.RawMessage
}
