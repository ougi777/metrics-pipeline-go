package messaging

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
)

type relayRepositoryFake struct {
	mu           sync.Mutex
	acquired     bool
	event        domain.OutboxEvent
	claimed      int
	failed       int
	published    int
	released     int
	batch        []domain.OutboxEvent
	batchClaimed bool
	lastBackoff  time.Duration
}

func (f *relayRepositoryFake) Claim(context.Context, int, time.Duration) ([]domain.OutboxEvent, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.batch) > 0 {
		if f.batchClaimed {
			return nil, "", nil
		}
		f.batchClaimed = true
		f.claimed++
		return f.batch, f.batch[0].ClaimToken, nil
	}
	if f.published > 0 {
		return nil, "", nil
	}
	f.claimed++
	return []domain.OutboxEvent{f.event}, f.event.ClaimToken, nil
}

func (f *relayRepositoryFake) MarkPublished(context.Context, domain.OutboxEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published++
	return nil
}

func (f *relayRepositoryFake) MarkFailed(_ context.Context, _ domain.OutboxEvent, backoff time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed++
	f.lastBackoff = backoff
	return nil
}

func (f *relayRepositoryFake) ReleaseClaim(_ context.Context, _ domain.OutboxEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released++
	return nil
}

func (f *relayRepositoryFake) TryAcquireLeader(context.Context) (func(), bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.acquired {
		return func() {}, true, nil
	}
	return func() {}, false, nil
}

type relayPublisherFake struct {
	mu       sync.Mutex
	attempts int
	failures int
}

func (f *relayPublisherFake) Publish(context.Context, domain.OutboxEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.attempts <= f.failures {
		return errors.New("temporary publish failure")
	}
	return nil
}

func (f *relayPublisherFake) Close() error { return nil }

func TestOutboxRelayPublishesAndMarksPublished(t *testing.T) {
	repository := &relayRepositoryFake{acquired: true, event: domain.OutboxEvent{ID: 1, TaskID: "task-a", EventSeq: 1, ClaimToken: "token"}}
	publisher := &relayPublisherFake{}
	relay, err := NewOutboxRelay(OutboxRelayConfig{PollInterval: time.Millisecond}, repository, publisher, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.published == 1
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if publisher.attempts != 1 {
		t.Fatalf("publish attempts = %d, want 1", publisher.attempts)
	}
}

func TestOutboxRelayRetriesFailureAndPreservesIntent(t *testing.T) {
	repository := &relayRepositoryFake{acquired: true, event: domain.OutboxEvent{ID: 2, TaskID: "task-a", EventSeq: 2, ClaimToken: "token"}}
	publisher := &relayPublisherFake{failures: 1}
	relay, err := NewOutboxRelay(OutboxRelayConfig{PollInterval: time.Millisecond, InitialBackoff: time.Millisecond}, repository, publisher, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.published == 1
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if repository.failed != 1 || publisher.attempts < 2 {
		t.Fatalf("failure state = failed:%d attempts:%d", repository.failed, publisher.attempts)
	}
}

func TestOutboxRelayReleasesUnprocessedBatchAfterPublishFailure(t *testing.T) {
	events := []domain.OutboxEvent{
		{ID: 1, TaskID: "task-a", EventSeq: 1, ClaimToken: "token"},
		{ID: 2, TaskID: "task-a", EventSeq: 2, ClaimToken: "token"},
		{ID: 3, TaskID: "task-a", EventSeq: 3, ClaimToken: "token"},
	}
	repository := &relayRepositoryFake{acquired: true, batch: events}
	publisher := &relayPublisherFake{failures: 1}
	relay, err := NewOutboxRelay(OutboxRelayConfig{InitialBackoff: time.Nanosecond}, repository, publisher, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := relay.runLeader(context.Background()); err != nil {
		t.Fatalf("runLeader() error = %v", err)
	}
	if publisher.attempts != 1 {
		t.Fatalf("publish attempts = %d, want 1", publisher.attempts)
	}
	if repository.failed != 1 {
		t.Fatalf("failed count = %d, want 1", repository.failed)
	}
	if repository.released != 2 {
		t.Fatalf("released claims = %d, want 2", repository.released)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not reached")
}
