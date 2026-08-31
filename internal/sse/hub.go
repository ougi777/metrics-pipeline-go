package sse

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
)

type Event struct {
	TaskID   string
	EventSeq int64
	Payload  json.RawMessage
}

type Subscription struct {
	C     <-chan Event
	close func()
}

func (s Subscription) Close() { s.close() }

type Hub struct {
	mu     sync.RWMutex
	nextID uint64
	subs   map[string]map[uint64]chan Event
}

func NewHub() *Hub { return &Hub{subs: make(map[string]map[uint64]chan Event)} }

func (h *Hub) Subscribe(taskID string) Subscription {
	h.mu.Lock()
	h.nextID++
	id := h.nextID
	ch := make(chan Event, 64)
	if h.subs[taskID] == nil {
		h.subs[taskID] = make(map[uint64]chan Event)
	}
	h.subs[taskID][id] = ch
	h.mu.Unlock()
	return Subscription{C: ch, close: func() {
		h.mu.Lock()
		if channels, ok := h.subs[taskID]; ok {
			delete(channels, id)
			close(ch)
			if len(channels) == 0 {
				delete(h.subs, taskID)
			}
		}
		h.mu.Unlock()
	}}
}

func (h *Hub) HandleMetricEvent(ctx context.Context, event domain.RealtimeEvent) error {
	return h.Publish(ctx, Event{TaskID: event.TaskID, EventSeq: event.EventSeq, Payload: event.Payload})
}

func (h *Hub) Publish(ctx context.Context, event Event) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.subs[event.TaskID] {
		select {
		case ch <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
