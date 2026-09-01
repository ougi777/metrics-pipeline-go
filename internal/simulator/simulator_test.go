package simulator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{Endpoint: "http://example.test", Tasks: 1, Rate: 10, Duration: time.Second, BatchSize: 4, TaskPrefix: "sim", EvalEvery: 3, EvalLoss: true, GPUUtil: true, GPUMem: true, Throughput: true, Seed: 7}
}

func TestGeneratorProducesStableSeries(t *testing.T) {
	cfg := testConfig()
	g := NewGenerator("sim-0001", time.UnixMilli(1756089600123), cfg, cfg.Seed)
	batch := g.NextBatch(8)
	if len(batch.Samples) != 8 {
		t.Fatalf("samples = %d, want 8", len(batch.Samples))
	}
	for i, sample := range batch.Samples {
		if sample.Step != int64(i) {
			t.Errorf("step[%d] = %d", i, sample.Step)
		}
		if i > 0 && sample.TimestampMillis <= batch.Samples[i-1].TimestampMillis {
			t.Errorf("timestamp[%d] is not increasing", i)
		}
		for _, key := range []string{"loss", "lr", "gpu_util", "gpu_mem", "throughput"} {
			if _, ok := sample.Metrics[key]; !ok {
				t.Errorf("sample[%d] missing %s", i, key)
			}
		}
		_, eval := sample.Metrics["eval_loss"]
		if eval != (i%cfg.EvalEvery == 0) {
			t.Errorf("eval_loss at step %d = %v", i, eval)
		}
	}
}

func TestGeneratorOptionalMetrics(t *testing.T) {
	cfg := Config{Rate: 1, EvalEvery: 10}
	g := NewGenerator("x", time.UnixMilli(1000), cfg, 1)
	metrics := g.NextBatch(1).Samples[0].Metrics
	if len(metrics) != 2 {
		t.Fatalf("metric count = %d, want 2", len(metrics))
	}
}

func TestRunPostsBatches(t *testing.T) {
	requests := make(chan int, 10)
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		requests <- 1
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cfg := testConfig()
	cfg.Endpoint = server.URL
	cfg.Duration = 20 * time.Millisecond
	cfg.Rate = 100
	cfg.BatchSize = 1
	if err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if len(requests) == 0 {
		t.Fatal("expected at least one request")
	}
	if _, ok := payload["task_id"]; !ok {
		t.Fatal("request missing task_id")
	}
	batch, ok := payload["batch"].([]any)
	if !ok || len(batch) == 0 {
		t.Fatal("request missing batch")
	}
	sample := batch[0].(map[string]any)
	for _, key := range []string{"step", "ts", "metrics"} {
		if _, ok := sample[key]; !ok {
			t.Errorf("sample missing %s", key)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := testConfig()
	cfg.Tasks = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestCompareAuditPassesExpectedValues(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/tasks/sim-0001/audit" {
			t.Errorf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"task_id":"sim-0001","point_count":4,"distinct_steps":2,"first_step":0,"last_step":1,"keys":["loss","lr"],"missing_steps":[]}`))
	}))
	defer server.Close()
	result := AuditResult{TaskID: "sim-0001", Expected: Expected{TaskID: "sim-0001", PointCount: 4, DistinctSteps: 2, FirstStep: ptr(0), LastStep: ptr(1), Keys: []string{"loss", "lr"}}}
	compareAudit(context.Background(), server.Client(), auditEndpoint(server.URL+"/api/v1/ingest/metrics"), &result)
	if !result.Pass || len(result.Differences) != 0 {
		t.Fatalf("result = %+v", result)
	}
}

func TestAuditReportWaitsForConvergence(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			_, _ = w.Write([]byte(`{"task_id":"sim-0001","point_count":2,"distinct_steps":1,"first_step":0,"last_step":0,"keys":["loss","lr"],"missing_steps":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"task_id":"sim-0001","point_count":4,"distinct_steps":2,"first_step":0,"last_step":1,"keys":["loss","lr"],"missing_steps":[]}`))
	}))
	defer server.Close()
	results := []AuditResult{{TaskID: "sim-0001", Expected: Expected{TaskID: "sim-0001", PointCount: 4, DistinctSteps: 2, FirstStep: ptr(0), LastStep: ptr(1), Keys: []string{"loss", "lr"}}}}
	auditReport(context.Background(), server.Client(), auditEndpoint(server.URL+"/api/v1/ingest/metrics"), results, time.Second, time.Millisecond)
	if !results[0].Pass || requests < 2 {
		t.Fatalf("result = %+v, requests = %d", results[0], requests)
	}
}

func ptr(value int64) *int64 { return &value }

func TestExpectedStateDeduplicatesReplay(t *testing.T) {
	cfg := Config{Rate: 1, EvalEvery: 10}
	gen := NewGenerator("sim-0001", time.UnixMilli(1000), cfg, 1)
	batch := gen.NextBatch(2)
	state := newExpectedState(batch.TaskID)
	fingerprint := batchFingerprint(batch)
	for i := 0; i < 2; i++ {
		if !state.batches[fingerprint] {
			state.batches[fingerprint] = true
			addExpected(&state, batch)
		}
	}
	expected := state.finish()
	if expected.BatchItems != 2 || expected.PointCount != 4 || expected.DistinctSteps != 2 {
		t.Fatalf("expected = %+v", expected)
	}
}
