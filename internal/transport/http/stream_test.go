package httptransport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ougi777/metrics-pipeline-go/internal/service/events"
	"github.com/ougi777/metrics-pipeline-go/internal/sse"
)

func TestParseLastEventID(t *testing.T) {
	seq, ok, err := parseLastEventID("task-a:12", "task-a")
	if err != nil || !ok || seq != 12 {
		t.Fatalf("parse = %d/%v/%v", seq, ok, err)
	}
	for _, value := range []string{"task-b:12", "task-a", "task-a:x", "task-a:-1", "task-a:1:2"} {
		if _, _, err := parseLastEventID(value, "task-a"); err == nil {
			t.Errorf("parseLastEventID(%q) accepted invalid value", value)
		}
	}
}

type streamRepo struct{}

func (streamRepo) QueryEvents(_ context.Context, taskID string, after int64) ([]sse.Event, error) {
	if after >= 1 {
		return nil, nil
	}
	return []sse.Event{{TaskID: taskID, EventSeq: 1, Payload: []byte(`{"points":[]}`)}}, nil
}
func (streamRepo) EventBounds(context.Context, string) (int64, int64, error) { return 1, 1, nil }

func TestStreamMetricsReplaysThenReceivesLiveEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hub := sse.NewHub()
	handler := NewStreamHandler(events.NewService(streamRepo{}), hub)
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task-a/metrics/stream", nil).WithContext(ctx)
	request.Header.Set("Last-Event-ID", "task-a:0")
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = request
	ginContext.Params = gin.Params{{Key: "task_id", Value: "task-a"}}
	done := make(chan struct{})
	go func() { handler.StreamMetrics(ginContext); close(done) }()
	time.Sleep(20 * time.Millisecond)
	if err := hub.Publish(context.Background(), sse.Event{TaskID: "task-a", EventSeq: 2, Payload: []byte(`{"points":[]}`)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream handler did not stop after cancellation")
	}
	body, _ := io.ReadAll(recorder.Result().Body)
	text := string(body)
	if !strings.Contains(text, "id: task-a:1") || !strings.Contains(text, "id: task-a:2") {
		t.Fatalf("stream body = %q", text)
	}
}
