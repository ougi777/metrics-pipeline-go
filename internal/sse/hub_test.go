package sse

import (
	"context"
	"testing"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
)

func TestHubPublishesOnlyToMatchingTaskAndClosesSubscription(t *testing.T) {
	hub := NewHub()
	a := hub.Subscribe("task-a")
	b := hub.Subscribe("task-b")
	if err := hub.HandleMetricEvent(context.Background(), domain.RealtimeEvent{TaskID: "task-a", EventSeq: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-a.C:
		if event.EventSeq != 1 {
			t.Fatalf("event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("task-a subscriber did not receive event")
	}
	select {
	case event := <-b.C:
		t.Fatalf("task-b received event %#v", event)
	default:
	}
	a.Close()
	select {
	case _, ok := <-a.C:
		if ok {
			t.Fatal("subscription channel remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription channel did not close")
	}
	b.Close()
}
