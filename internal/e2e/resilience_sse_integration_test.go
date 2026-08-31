//go:build integration

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestResilienceAndSSEEndToEnd(t *testing.T) {
	apiURL := strings.TrimRight(envOr("API_INTEGRATION_URL", "http://localhost:8080"), "/")
	databaseURL := envOr("DATABASE_INTEGRATION_URL", "postgres://metrics:metrics@localhost:5432/metrics?sslmode=disable")
	client := &http.Client{Timeout: 10 * time.Second}
	adminURL := strings.TrimRight(envOr("API_ADMIN_INTEGRATION_URL", "http://localhost:8081"), "/")
	waitForHTTP(t, client, apiURL+"/api/v1/tasks/e2e-health-check/summary", http.StatusNotFound)

	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("ping integration database: %v", err)
	}
	taskID := fmt.Sprintf("resilience-e2e-%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanupTask(t, pool, taskID) })

	// Force-stop the worker while publishing. RabbitMQ retains the message for recovery.
	compose(t, "kill", "worker")
	t.Cleanup(func() {
		cmd := exec.Command("docker", "compose", "up", "--detach", "--wait", "worker")
		cmd.Dir = composeRoot(t)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Logf("restore worker after resilience test: %v\n%s", err, bytes.TrimSpace(output))
		}
	})
	firstTS := time.Now().UTC().Truncate(time.Millisecond).UnixMilli()
	first := ingestRequest{TaskID: taskID, Batch: []ingestSample{{Step: 1, TS: firstTS, Metrics: map[string]float64{"loss": 3, "lr": 0.1}}}}
	status, _ := postJSON(t, client, apiURL+"/api/v1/ingest/metrics", first)
	if status != http.StatusOK {
		t.Fatalf("ingest while worker stopped status=%d, want 200", status)
	}
	compose(t, "up", "--detach", "--wait", "worker")
	waitForCounts(t, pool, taskID, 2, 1, 1)

	// Subscribe from the beginning and verify the live event ID, ordering and JSON payload.
	streamCtx, cancelStream := context.WithCancel(context.Background())
	streamReq, err := http.NewRequestWithContext(streamCtx, http.MethodGet, apiURL+"/api/v1/tasks/"+taskID+"/metrics/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	streamReq.Header.Set("Last-Event-ID", taskID+":0")
	streamResp, err := client.Do(streamReq)
	if err != nil {
		cancelStream()
		t.Fatalf("open SSE stream: %v", err)
	}
	if streamResp.StatusCode != http.StatusOK {
		streamResp.Body.Close()
		cancelStream()
		t.Fatalf("SSE status=%d, want 200", streamResp.StatusCode)
	}
	t.Cleanup(func() {
		cancelStream()
		_ = streamResp.Body.Close()
	})
	firstEvent := make(chan sseEvent, 1)
	go func() { firstEvent <- readSSEEvent(streamResp.Body) }()
	event := awaitSSE(t, firstEvent)
	if event.ID != taskID+":1" || !json.Valid([]byte(event.Data)) || !strings.Contains(event.Data, `"points"`) {
		t.Fatalf("first SSE event=%+v", event)
	}
	cancelStream()

	// Publish an event while disconnected, then replay it after an API restart.
	second := ingestRequest{TaskID: taskID, Batch: []ingestSample{{Step: 2, TS: firstTS + 1000, Metrics: map[string]float64{"loss": 2, "lr": 0.2}}}}
	status, _ = postJSON(t, client, apiURL+"/api/v1/ingest/metrics", second)
	if status != http.StatusOK {
		t.Fatalf("second ingest status=%d, want 200", status)
	}
	waitForCounts(t, pool, taskID, 4, 2, 2)
	compose(t, "restart", "api")
	waitForHTTP(t, client, adminURL+"/readyz", http.StatusOK)

	replayCtx, cancelReplay := context.WithCancel(context.Background())
	defer cancelReplay()
	replayReq, err := http.NewRequestWithContext(replayCtx, http.MethodGet, apiURL+"/api/v1/tasks/"+taskID+"/metrics/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	replayReq.Header.Set("Last-Event-ID", taskID+":1")
	replayResp, err := client.Do(replayReq)
	if err != nil {
		t.Fatalf("open replay SSE stream: %v", err)
	}
	defer replayResp.Body.Close()
	if replayResp.StatusCode != http.StatusOK {
		t.Fatalf("replay SSE status=%d, want 200", replayResp.StatusCode)
	}
	replayed := make(chan sseEvent, 1)
	go func() { replayed <- readSSEEvent(replayResp.Body) }()
	replayEvent := awaitSSE(t, replayed)
	if replayEvent.ID != taskID+":2" || !json.Valid([]byte(replayEvent.Data)) || !strings.Contains(replayEvent.Data, `"points"`) {
		t.Fatalf("replayed SSE event=%+v", replayEvent)
	}
}

type sseEvent struct {
	ID   string
	Data string
}

func readSSEEvent(body io.Reader) sseEvent {
	scanner := bufio.NewScanner(body)
	event := sseEvent{}
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			event.ID = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			event.Data = strings.TrimPrefix(line, "data: ")
		}
		if event.ID != "" && event.Data != "" {
			return event
		}
	}
	return event
}

func awaitSSE(t *testing.T, events <-chan sseEvent) sseEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for SSE event")
		return sseEvent{}
	}
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = composeRoot(t)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, bytes.TrimSpace(output))
	}
}

func composeRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get test working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "compose.yaml")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("compose.yaml not found")
	return ""
}
