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
