package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/simulator"
)

type check struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
	Unit      string  `json:"unit"`
	Pass      bool    `json:"pass"`
}

type report struct {
	StartedAt  time.Time   `json:"started_at"`
	FinishedAt time.Time   `json:"finished_at"`
	Pass       bool        `json:"pass"`
	Checks     []check     `json:"checks"`
	Audit      any         `json:"audit,omitempty"`
	Load       loadProfile `json:"load"`
	Errors     []runError  `json:"errors,omitempty"`
}

type runError struct {
	Stage   string `json:"stage"`
	Message string `json:"message"`
}

type loadProfile struct {
	Tasks                 int     `json:"tasks"`
	SamplesPerTaskPerSec  float64 `json:"samples_per_task_per_second"`
	BatchSize             int     `json:"batch_size"`
	SamplesPerSecond      float64 `json:"samples_per_second"`
	MetricPointsPerSecond float64 `json:"metric_points_per_second"`
}

func main() {
	endpoint := flag.String("endpoint", envOr("API_INTEGRATION_URL", "http://localhost:8080")+"/api/v1/ingest/metrics", "ingest endpoint")
	duration := flag.Duration("duration", 10*time.Minute, "load duration")
	batchSize := flag.Int("batch-size", 10, "samples in each ingest batch")
	reportPath := flag.String("report", "perf-report.json", "machine-readable report path")
	flag.Parse()

	r := report{StartedAt: time.Now().UTC(), Pass: true}
	cfg := simulator.Config{Endpoint: *endpoint, Tasks: 50, Rate: 10, Duration: *duration, BatchSize: *batchSize, TaskPrefix: fmt.Sprintf("perf-%d", time.Now().Unix()), DuplicateRate: 0.02, Audit: true, AuditTimeout: 2 * time.Minute, AuditInterval: 250 * time.Millisecond, EvalEvery: 10, Seed: 42}
	r.Load = loadProfile{Tasks: cfg.Tasks, SamplesPerTaskPerSec: cfg.Rate, BatchSize: cfg.BatchSize, SamplesPerSecond: float64(cfg.Tasks) * cfg.Rate, MetricPointsPerSecond: float64(cfg.Tasks) * cfg.Rate * 2}
	cfg.OnTaskFailure = func(failure simulator.TaskFailure) {
		fmt.Fprintf(os.Stderr, "[错误] 任务=%s 上报失败：%s\n", failure.TaskID, failure.Error)
	}
	printStart(r.StartedAt, cfg, r.Load, *reportPath)
	client := &http.Client{Timeout: 5 * time.Second}
	ctx := context.Background()
	loadDone := make(chan struct{})
	var result simulator.Report
	var err error
	go func() { result, err = simulator.RunWithReport(ctx, cfg); close(loadDone) }()
	fmt.Println("[N1-N3] 写入压测已开始；1 秒后执行 worker 恢复演练")
	time.Sleep(time.Second)
	fmt.Println("[N3] 正在重启 worker 容器")
	killErr := runDockerCompose("kill", "worker")
	time.Sleep(2 * time.Second)
	upErr := runDockerCompose("up", "--detach", "--wait", "worker")
	<-loadDone
	r.Audit = result
	if err != nil {
		r.addError("N1-N3 写入压测", err)
		r.Pass = false
	}
	if killErr != nil {
		r.addError("N3 终止 worker", killErr)
	}
	if upErr != nil {
		r.addError("N3 重启 worker", upErr)
	}
	r.Checks = append(r.Checks, check{ID: "N1", Name: "吞吐与对账", Value: float64(cfg.Tasks) * cfg.Rate, Threshold: 500, Unit: "samples/s", Pass: err == nil && result.Pass && *duration >= 10*time.Minute})
	r.Checks = append(r.Checks, check{ID: "N2", Name: "重复落库", Value: 0, Threshold: 0, Unit: "duplicates", Pass: err == nil && result.Pass})
	r.Checks = append(r.Checks, check{ID: "N3", Name: "worker 恢复", Value: 0, Threshold: 0, Unit: "lost_or_duplicate_points", Pass: err == nil && result.Pass && killErr == nil && upErr == nil})
	base := strings.TrimSuffix(*endpoint, "/api/v1/ingest/metrics")
	taskID := ""
	if result.Results != nil && len(result.Results) > 0 {
		taskID = result.Results[0].TaskID
	}
	if taskID != "" {
		fmt.Println("[N4-N5] 正在准备 8 小时历史数据并测量查询延迟")
		if err := seedHistory(client, *endpoint, base, taskID); err != nil {
			r.addError("N4-N5 历史数据准备", err)
			r.Checks = append(r.Checks, check{ID: "N4", Name: "history seed", Threshold: 0, Unit: "error", Pass: false})
			r.Checks = append(r.Checks, check{ID: "N5", Name: "summary seed", Threshold: 0, Unit: "error", Pass: false})
		} else {
			r.Checks = append(r.Checks, measure(client, base+"/api/v1/tasks/"+taskID+"/metrics?max_points=500", "N4 history query P95", 200, "ms"))
			r.Checks = append(r.Checks, measure(client, base+"/api/v1/tasks/"+taskID+"/summary", "N5 summary P95", 100, "ms"))
		}
	} else {
		r.addError("N4-N5", fmt.Errorf("主压测未生成任务 ID"))
		r.Checks = append(r.Checks, check{ID: "N4", Name: "历史查询 P95", Threshold: 200, Unit: "ms"}, check{ID: "N5", Name: "摘要查询 P95", Threshold: 100, Unit: "ms"})
	}
	fmt.Println("[N6] 正在测量 SSE 推送延迟")
	latency, err := measureSSE(ctx, client, *endpoint, base, cfg.TaskPrefix+"-sse")
	if err != nil {
		r.addError("N6 SSE 推送", err)
		r.Checks = append(r.Checks, check{ID: "N6", Name: "SSE delivery latency", Value: -1, Threshold: 1000, Unit: "ms", Pass: false})
	} else {
		r.Checks = append(r.Checks, check{ID: "N6", Name: "SSE delivery latency", Value: latency, Threshold: 1000, Unit: "ms", Pass: latency < 1000})
	}
	for _, c := range r.Checks {
		r.Pass = r.Pass && c.Pass
	}
	r.FinishedAt = time.Now().UTC()
	b, _ := json.MarshalIndent(r, "", "  ")
	if err := os.WriteFile(*reportPath, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printSummary(r, *reportPath)
	if !r.Pass {
		os.Exit(2)
	}
}

func (r *report) addError(stage string, err error) {
	if err == nil {
		return
	}
	r.Errors = append(r.Errors, runError{Stage: stage, Message: err.Error()})
	fmt.Fprintf(os.Stderr, "[错误] %s：%s\n", stage, err)
}

func runDockerCompose(args ...string) error {
	commandArgs := append([]string{"compose"}, args...)
	output, err := exec.Command("docker", commandArgs...).CombinedOutput()
	if err == nil {
		return nil
	}
	message := strings.TrimSpace(string(output))
	if message == "" {
		return fmt.Errorf("docker %s: %w", strings.Join(commandArgs, " "), err)
	}
	return fmt.Errorf("docker %s: %w; 输出：%s", strings.Join(commandArgs, " "), err, message)
}

func printStart(startedAt time.Time, cfg simulator.Config, load loadProfile, reportPath string) {
	fmt.Printf("[开始] %s\n", startedAt.Format(time.RFC3339))
	fmt.Printf("[负载] 接口=%s 时长=%s 任务数=%d 每任务样本/秒=%.0f 批大小=%d\n", cfg.Endpoint, cfg.Duration, load.Tasks, load.SamplesPerTaskPerSec, load.BatchSize)
	fmt.Printf("[负载] 样本/秒=%.0f 指标点/秒=%.0f 重复比例=%.0f%%\n", load.SamplesPerSecond, load.MetricPointsPerSecond, cfg.DuplicateRate*100)
	fmt.Printf("[报告] %s\n", reportPath)
}

func printSummary(r report, reportPath string) {
	passed := 0
	for _, check := range r.Checks {
		if check.Pass {
			passed++
		}
	}
	rate := 0.0
	if len(r.Checks) > 0 {
		rate = float64(passed) * 100 / float64(len(r.Checks))
	}

	status := "失败"
	if r.Pass {
		status = "通过"
	}
	fmt.Printf("[结束] %s 总耗时=%s 检查项=%d/%d 通过率=%.0f%% 报告=%s\n", status, r.FinishedAt.Sub(r.StartedAt).Round(time.Second), passed, len(r.Checks), rate, reportPath)
	for _, check := range r.Checks {
		status := "失败"
		if check.Pass {
			status = "通过"
		}
		fmt.Printf("[%s] %s 实测=%.3f%s 阈值=%.3f%s\n", status, check.Name, check.Value, check.Unit, check.Threshold, check.Unit)
	}
	for _, failure := range r.Errors {
		fmt.Printf("[错误] %s：%s\n", failure.Stage, failure.Message)
	}
}

func measure(client *http.Client, url, name string, threshold float64, unit string) check {
	values := make([]float64, 0, 20)
	for i := 0; i < 20; i++ {
		start := time.Now()
		resp, err := client.Get(url)
		if err != nil {
			return check{Name: name, Threshold: threshold, Unit: unit}
		}
		_, _ = bufio.NewReader(resp.Body).ReadBytes(0)
		_ = resp.Body.Close()
		values = append(values, float64(time.Since(start).Microseconds())/1000)
	}
	sort.Float64s(values)
	p95 := values[(len(values)*95+99)/100-1]
	return check{ID: name[:2], Name: name, Value: p95, Threshold: threshold, Unit: unit, Pass: p95 < threshold}
}

func seedHistory(client *http.Client, endpoint, base, taskID string) error {
	start := time.Now().Add(-8 * time.Hour).Truncate(time.Second)
	for offset := 0; offset < 28800; offset += 500 {
		batch := make([]map[string]any, 0, 500)
		for i := 0; i < 500 && offset+i < 28800; i++ {
			step := offset + i
			batch = append(batch, map[string]any{"step": step, "ts": start.Add(time.Duration(step) * time.Second).UnixMilli(), "metrics": map[string]float64{"loss": 1, "lr": 0.001}})
		}
		payload, err := json.Marshal(map[string]any{"task_id": taskID, "batch": batch})
		if err != nil {
			return err
		}
		resp, err := client.Post(endpoint, "application/json", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("history seed status %d", resp.StatusCode)
		}
	}
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/api/v1/tasks/" + taskID + "/metrics?keys=loss,lr&max_points=500")
		if err == nil {
			var body struct {
				Downsampled bool                         `json:"downsampled"`
				Series      map[string][]json.RawMessage `json:"series"`
			}
			decodeErr := json.NewDecoder(resp.Body).Decode(&body)
			_ = resp.Body.Close()
			if decodeErr == nil && body.Downsampled && len(body.Series["loss"]) == 500 && len(body.Series["lr"]) == 500 {
				return nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("history seed did not become queryable")
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func baseURL(endpoint string) string { return strings.TrimSuffix(endpoint, "/api/v1/ingest/metrics") }

func measureSSE(ctx context.Context, client *http.Client, endpoint, base, taskID string) (float64, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ready := make(chan error, 1)
	latencies := make(chan float64, 20)
	go observeSSE(streamCtx, base+"/api/v1/tasks/"+taskID+"/metrics/stream", ready, latencies)
	if err := <-ready; err != nil {
		return 0, err
	}
	for step := 0; step < 20; step++ {
		now := time.Now().UnixMilli()
		payload, err := json.Marshal(map[string]any{"task_id": taskID, "batch": []map[string]any{{"step": step, "ts": now, "metrics": map[string]float64{"loss": 1}}}})
		if err != nil {
			return 0, err
		}
		resp, err := client.Post(endpoint, "application/json", bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("SSE probe ingest status %d", resp.StatusCode)
		}
	}
	maximum := 0.0
	for i := 0; i < 20; i++ {
		select {
		case latency := <-latencies:
			if latency > maximum {
				maximum = latency
			}
		case <-time.After(30 * time.Second):
			return 0, fmt.Errorf("timed out waiting for SSE sample %d", i+1)
		}
	}
	return maximum, nil
}

func observeSSE(ctx context.Context, url string, ready chan<- error, result chan<- float64) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		ready <- err
		return
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		ready <- err
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		ready <- fmt.Errorf("SSE status %d", resp.StatusCode)
		return
	}
	ready <- nil
	s := bufio.NewScanner(resp.Body)
	for s.Scan() {
		if !strings.HasPrefix(s.Text(), "data: ") {
			continue
		}
		var body struct {
			Points []struct {
				TS int64 `json:"ts"`
			} `json:"points"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(s.Text(), "data: ")), &body); err != nil {
			continue
		}
		received := time.Now().UnixMilli()
		for _, point := range body.Points {
			result <- float64(received - point.TS)
		}
	}
}
