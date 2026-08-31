//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ingestRequest struct {
	TaskID string         `json:"task_id"`
	Batch  []ingestSample `json:"batch"`
}

type ingestSample struct {
	Step    int64              `json:"step"`
	TS      int64              `json:"ts"`
	Metrics map[string]float64 `json:"metrics"`
}

func TestCorePipelineEndToEnd(t *testing.T) {
	apiURL := envOr("API_INTEGRATION_URL", "http://localhost:8080")
	apiURL = strings.TrimRight(apiURL, "/")
	databaseURL := envOr("DATABASE_INTEGRATION_URL", "postgres://metrics:metrics@localhost:5432/metrics?sslmode=disable")
	client := &http.Client{Timeout: 5 * time.Second}

	waitForHTTP(t, client, apiURL+"/api/v1/tasks/health-check/summary", http.StatusNotFound)

	taskID := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	missingTaskID := taskID + "-missing"
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping integration database: %v", err)
	}
	t.Cleanup(func() { cleanupTask(t, pool, taskID) })

	baseTS := time.Now().UTC().Truncate(time.Millisecond).UnixMilli()
	request := ingestRequest{TaskID: taskID, Batch: []ingestSample{
		{Step: 1, TS: baseTS, Metrics: map[string]float64{"loss": 3, "lr": 0.1}},
		{Step: 3, TS: baseTS + 1000, Metrics: map[string]float64{"loss": 1, "lr": 0.3}},
	}}
	status, body := postJSON(t, client, apiURL+"/api/v1/ingest/metrics", request)
	if status != http.StatusOK || body["accepted"] != float64(2) || body["task_id"] != taskID {
		t.Fatalf("ingest response status=%d body=%v", status, body)
	}

	// A second submission of the same batch must remain idempotent.
	status, _ = postJSON(t, client, apiURL+"/api/v1/ingest/metrics", request)
	if status != http.StatusOK {
		t.Fatalf("duplicate ingest status=%d, want 200", status)
	}
	waitForCounts(t, pool, taskID, 4, 1, 1)

	status, history := getJSON(t, client, apiURL+"/api/v1/tasks/"+taskID+"/metrics?keys=loss&from="+fmt.Sprint(baseTS)+"&to="+fmt.Sprint(baseTS+2000)+"&step_from=1&step_to=3&max_points=1")
	series, ok := history["series"].(map[string]any)
	if status != http.StatusOK || !ok || len(series["loss"].([]any)) == 0 || history["downsampled"] != true {
		t.Fatalf("history response status=%d body=%v", status, history)
	}

	status, empty := getJSON(t, client, apiURL+"/api/v1/tasks/"+taskID+"/metrics?keys=accuracy")
	if status != http.StatusOK || len(empty["series"].(map[string]any)) != 0 {
		t.Fatalf("empty history status=%d body=%v", status, empty)
	}

	status, summary := getJSON(t, client, apiURL+"/api/v1/tasks/"+taskID+"/summary")
	if status != http.StatusOK || summary["last_step"] != float64(3) {
		t.Fatalf("summary status=%d body=%v", status, summary)
	}
	metrics := summary["metrics"].(map[string]any)
	if metrics["loss"].(map[string]any)["avg"] != 2.0 {
		t.Fatalf("summary metrics=%v", metrics)
	}

	status, audit := getJSON(t, client, apiURL+"/api/v1/admin/tasks/"+taskID+"/audit")
	if status != http.StatusOK || audit["point_count"] != float64(4) || audit["distinct_steps"] != float64(2) {
		t.Fatalf("audit status=%d body=%v", status, audit)
	}
	if missing := audit["missing_steps"].([]any); len(missing) != 1 || missing[0] != float64(2) {
		t.Fatalf("missing steps=%v", missing)
	}

	status, _ = postJSON(t, client, apiURL+"/api/v1/ingest/metrics", map[string]any{"task_id": taskID, "batch": []any{}})
	if status != http.StatusBadRequest {
		t.Fatalf("empty batch status=%d, want 400", status)
	}
	status, _ = getJSON(t, client, apiURL+"/api/v1/tasks/"+missingTaskID+"/summary")
	if status != http.StatusNotFound {
		t.Fatalf("missing task summary status=%d, want 404", status)
	}
}

func waitForHTTP(t *testing.T, client *http.Client, url string, expected int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == expected {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("service at %s did not return status %d", url, expected)
}

func waitForCounts(t *testing.T, pool *pgxpool.Pool, taskID string, points, events, outbox int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var gotPoints, gotEvents, gotOutbox int
		err := pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM metric_points WHERE task_id=$1), (SELECT count(*) FROM metric_events WHERE task_id=$1), (SELECT count(*) FROM metric_outbox WHERE task_id=$1)`, taskID).Scan(&gotPoints, &gotEvents, &gotOutbox)
		if err == nil && gotPoints == points && gotEvents == events && gotOutbox == outbox {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("timed out waiting for worker persistence")
}

func postJSON(t *testing.T, client *http.Client, url string, value any) (int, map[string]any) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode, decodeJSON(t, resp.Body)
}

func getJSON(t *testing.T, client *http.Client, url string) (int, map[string]any) {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode, decodeJSON(t, resp.Body)
}

func decodeJSON(t *testing.T, reader io.Reader) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.NewDecoder(reader).Decode(&value); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return value
}

func cleanupTask(t *testing.T, pool *pgxpool.Pool, taskID string) {
	t.Helper()
	for _, table := range []string{"metric_outbox", "metric_events", "metric_points", "task_event_counters"} {
		if _, err := pool.Exec(context.Background(), "DELETE FROM "+table+" WHERE task_id = $1", taskID); err != nil {
			t.Errorf("cleanup %s: %v", table, err)
		}
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
