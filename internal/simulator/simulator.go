// Package simulator generates training-like metric batches and posts them to the ingest API.
package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
)

var taskPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

type Config struct {
	Endpoint      string
	Tasks         int
	Rate          float64
	Duration      time.Duration
	BatchSize     int
	TaskPrefix    string
	EvalLoss      bool
	EvalEvery     int
	GPUUtil       bool
	GPUMem        bool
	Throughput    bool
	Seed          int64
	Client        *http.Client
	DuplicateRate float64
	FailureRate   float64
	Audit         bool
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Endpoint) == "" {
		return fmt.Errorf("endpoint must not be empty")
	}
	if c.Tasks <= 0 {
		return fmt.Errorf("tasks must be greater than zero")
	}
	if c.Rate <= 0 {
		return fmt.Errorf("rate must be greater than zero")
	}
	if c.Duration <= 0 {
		return fmt.Errorf("duration must be greater than zero")
	}
	if c.BatchSize <= 0 || c.BatchSize > 500 {
		return fmt.Errorf("batch size must be between 1 and 500")
	}
	if c.EvalEvery <= 0 {
		return fmt.Errorf("eval every must be greater than zero")
	}
	if len(c.TaskPrefix) == 0 || len(c.TaskPrefix) > 48 || !taskPrefixPattern.MatchString(c.TaskPrefix) {
		return fmt.Errorf("task prefix must start with an alphanumeric character and contain only alphanumerics, '.', '_' or '-'")
	}
	if c.DuplicateRate < 0 || c.DuplicateRate > 1 {
		return fmt.Errorf("duplicate rate must be between 0 and 1")
	}
	if c.FailureRate < 0 || c.FailureRate > 1 {
		return fmt.Errorf("failure rate must be between 0 and 1")
	}
	return nil
}

type Expected struct {
	TaskID        string   `json:"task_id"`
	BatchItems    int      `json:"batch_items"`
	PointCount    int      `json:"point_count"`
	DistinctSteps int      `json:"distinct_steps"`
	FirstStep     *int64   `json:"first_step"`
	LastStep      *int64   `json:"last_step"`
	Keys          []string `json:"keys"`
	MissingSteps  []int64  `json:"missing_steps"`
}
type expectedState struct {
	Expected
	steps   map[int64]bool
	batches map[string]bool
}
type AuditResponse struct {
	TaskID        string   `json:"task_id"`
	PointCount    int64    `json:"point_count"`
	DistinctSteps int64    `json:"distinct_steps"`
	FirstStep     *int64   `json:"first_step"`
	LastStep      *int64   `json:"last_step"`
	Keys          []string `json:"keys"`
	MissingSteps  []int64  `json:"missing_steps"`
}
type AuditResult struct {
	TaskID      string         `json:"task_id"`
	Pass        bool           `json:"pass"`
	Expected    Expected       `json:"expected"`
	Actual      *AuditResponse `json:"actual,omitempty"`
	Differences []string       `json:"differences,omitempty"`
	Error       string         `json:"error,omitempty"`
}
type Report struct {
	Pass    bool          `json:"pass"`
	Results []AuditResult `json:"results"`
}

type Generator struct {
	TaskID                            string
	step                              int64
	nextTS                            int64
	interval                          time.Duration
	rng                               *rand.Rand
	loss, gpuUtil, gpuMem, throughput float64
	config                            Config
}

func NewGenerator(taskID string, start time.Time, cfg Config, seed int64) *Generator {
	interval := time.Duration(float64(time.Second) / cfg.Rate)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	return &Generator{TaskID: taskID, nextTS: start.UnixMilli(), interval: interval, rng: rand.New(rand.NewSource(seed)), loss: 2, gpuUtil: 70, gpuMem: 55, throughput: 900, config: cfg}
}

func (g *Generator) NextBatch(size int) domain.MetricBatch {
	if size < 1 {
		size = 1
	}
	batch := domain.MetricBatch{TaskID: g.TaskID, Samples: make([]domain.MetricSample, 0, size)}
	for i := 0; i < size; i++ {
		step := g.step
		metrics := map[string]float64{
			"loss": math.Max(0, g.loss*math.Exp(-0.002*float64(step))+(g.rng.Float64()-0.5)*0.08),
			"lr":   g.learningRate(step),
		}
		if g.config.EvalLoss && step%int64(g.config.EvalEvery) == 0 {
			metrics["eval_loss"] = metrics["loss"]*1.05 + (g.rng.Float64()-0.5)*0.03
		}
		if g.config.GPUUtil {
			g.gpuUtil = clamp(g.gpuUtil+(g.rng.Float64()-0.5)*6, 0, 100)
			metrics["gpu_util"] = g.gpuUtil
		}
		if g.config.GPUMem {
			g.gpuMem = clamp(g.gpuMem+(g.rng.Float64()-0.5)*2, 0, 100)
			metrics["gpu_mem"] = g.gpuMem
		}
		if g.config.Throughput {
			g.throughput = math.Max(1, g.throughput+(g.rng.Float64()-0.5)*80)
			metrics["throughput"] = g.throughput
		}
		batch.Samples = append(batch.Samples, domain.MetricSample{Step: step, TimestampMillis: g.nextTS, Metrics: metrics})
		g.step++
		g.nextTS += g.interval.Milliseconds()
	}
	return batch
}

func (g *Generator) learningRate(step int64) float64 {
	const peak, warmup, period = 3e-4, 100.0, 2000.0
	if float64(step) < warmup {
		return peak * (float64(step) + 1) / warmup
	}
	progress := math.Mod(float64(step)-warmup, period) / period
	return peak * 0.5 * (1 + math.Cos(math.Pi*progress))
}

func clamp(v, low, high float64) float64 { return math.Max(low, math.Min(high, v)) }

func Run(ctx context.Context, cfg Config) error {
	_, err := RunWithReport(ctx, cfg)
	return err
}

func RunWithReport(ctx context.Context, cfg Config) (Report, error) {
	if err := cfg.Validate(); err != nil {
		return Report{}, err
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	var wg sync.WaitGroup
	errCh := make(chan error, cfg.Tasks)
	expectedCh := make(chan Expected, cfg.Tasks)
	//一个task启动一个协程
	for task := 0; task < cfg.Tasks; task++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			id := fmt.Sprintf("%s-%04d", cfg.TaskPrefix, index+1)
			gen := NewGenerator(id, time.Now(), cfg, cfg.Seed+int64(index))
			rng := rand.New(rand.NewSource(cfg.Seed + int64(index) + 1000000))
			var history []domain.MetricBatch
			expected := newExpectedState(id)
			defer func() { expectedCh <- expected.finish() }()
			deadline := time.NewTimer(cfg.Duration)
			defer deadline.Stop()
			ticker := time.NewTicker(batchInterval(cfg.Rate, cfg.BatchSize))
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-deadline.C:
					return
				default:
				}
				//生成指标
				batch := gen.NextBatch(cfg.BatchSize)
				if len(history) > 0 && rng.Float64() < cfg.DuplicateRate {
					batch = history[rng.Intn(len(history))]
				}
				if rng.Float64() >= cfg.FailureRate {
					//post到api接口
					if err := postBatch(ctx, client, cfg.Endpoint, batch); err != nil {
						select {
						case errCh <- fmt.Errorf("task %s: %w", id, err):
						default:
						}
						return
					}
					fingerprint := batchFingerprint(batch)
					history = append(history, batch)
					if !expected.batches[fingerprint] {
						expected.batches[fingerprint] = true
						addExpected(&expected, batch)
					}
				}
				select {
				case <-ctx.Done():
					return
				case <-deadline.C:
					return
				case <-ticker.C:
				}
			}
		}(task)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return Report{}, err
	default:
	}
	close(expectedCh)
	report := Report{Pass: true}
	for expected := range expectedCh {
		report.Results = append(report.Results, expectedResult(expected))
	}
	sort.Slice(report.Results, func(i, j int) bool { return report.Results[i].TaskID < report.Results[j].TaskID })
	if cfg.Audit {
		for i := range report.Results {
			compareAudit(ctx, client, auditEndpoint(cfg.Endpoint), &report.Results[i])
		}
	}
	for _, result := range report.Results {
		if !result.Pass {
			report.Pass = false
		}
	}
	return report, nil
}

func newExpectedState(id string) expectedState {
	return expectedState{Expected: Expected{TaskID: id}, steps: map[int64]bool{}, batches: map[string]bool{}}
}
func (e expectedState) finish() Expected {
	if e.FirstStep != nil {
		for step := *e.FirstStep; step <= *e.LastStep; step++ {
			if !e.steps[step] {
				e.MissingSteps = append(e.MissingSteps, step)
			}
		}
	}
	return e.Expected
}
func addExpected(e *expectedState, batch domain.MetricBatch) {
	for _, sample := range batch.Samples {
		e.BatchItems++
		e.PointCount += len(sample.Metrics)
		if e.FirstStep == nil || sample.Step < *e.FirstStep {
			v := sample.Step
			e.FirstStep = &v
		}
		e.steps[sample.Step] = true
		e.DistinctSteps = len(e.steps)
		if e.LastStep == nil || sample.Step > *e.LastStep {
			v := sample.Step
			e.LastStep = &v
		}
		for key := range sample.Metrics {
			if !contains(e.Keys, key) {
				e.Keys = append(e.Keys, key)
			}
		}
	}
	sort.Strings(e.Keys)
}
func batchFingerprint(batch domain.MetricBatch) string {
	if len(batch.Samples) == 0 {
		return ""
	}
	return fmt.Sprintf("%d:%d", batch.Samples[0].Step, batch.Samples[len(batch.Samples)-1].Step)
}
func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
func expectedResult(e Expected) AuditResult {
	return AuditResult{TaskID: e.TaskID, Pass: true, Expected: e}
}

func auditEndpoint(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return strings.TrimRight(endpoint, "/") + "/../../admin/tasks"
	}
	u.Path = strings.TrimSuffix(u.Path, "/ingest/metrics") + "/admin/tasks"
	return strings.TrimRight(u.String(), "/")
}

func compareAudit(ctx context.Context, client *http.Client, base string, result *AuditResult) {
	result.Pass = false
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/"+result.TaskID+"/audit", nil)
	if err != nil {
		result.Error = err.Error()
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		result.Error = err.Error()
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = fmt.Sprintf("audit returned status %d", resp.StatusCode)
		return
	}
	var actual AuditResponse
	if err := json.NewDecoder(resp.Body).Decode(&actual); err != nil {
		result.Error = err.Error()
		return
	}
	result.Actual = &actual
	if actual.TaskID != result.TaskID {
		result.Differences = append(result.Differences, fmt.Sprintf("task_id expected %s got %s", result.TaskID, actual.TaskID))
	}
	if actual.PointCount != int64(result.Expected.PointCount) {
		result.Differences = append(result.Differences, fmt.Sprintf("point_count expected %d got %d", result.Expected.PointCount, actual.PointCount))
	}
	if actual.DistinctSteps != int64(result.Expected.DistinctSteps) {
		result.Differences = append(result.Differences, fmt.Sprintf("distinct_steps expected %d got %d", result.Expected.DistinctSteps, actual.DistinctSteps))
	}
	if !equalIntPtr(actual.FirstStep, result.Expected.FirstStep) {
		result.Differences = append(result.Differences, "first_step differs")
	}
	if !equalIntPtr(actual.LastStep, result.Expected.LastStep) {
		result.Differences = append(result.Differences, "last_step differs")
	}
	if !equalStrings(actual.Keys, result.Expected.Keys) {
		result.Differences = append(result.Differences, "keys differ")
	}
	if !equalInt64s(actual.MissingSteps, result.Expected.MissingSteps) {
		result.Differences = append(result.Differences, "missing_steps differ")
	}
	result.Pass = len(result.Differences) == 0
}
func equalIntPtr(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
func equalStrings(a, b []string) bool { return strings.Join(a, "\x00") == strings.Join(b, "\x00") }
func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func batchInterval(rate float64, batchSize int) time.Duration {
	interval := time.Duration(float64(time.Second) * float64(batchSize) / rate)
	if interval < time.Millisecond {
		return time.Millisecond
	}
	return interval
}

func postBatch(ctx context.Context, client *http.Client, endpoint string, batch domain.MetricBatch) error {
	type samplePayload struct {
		Step    int64              `json:"step"`
		TS      int64              `json:"ts"`
		Metrics map[string]float64 `json:"metrics"`
	}
	samples := make([]samplePayload, 0, len(batch.Samples))
	for _, sample := range batch.Samples {
		samples = append(samples, samplePayload{Step: sample.Step, TS: sample.TimestampMillis, Metrics: sample.Metrics})
	}
	body, err := json.Marshal(struct {
		TaskID string          `json:"task_id"`
		Batch  []samplePayload `json:"batch"`
	}{batch.TaskID, samples})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ingest returned status %d", resp.StatusCode)
	}
	return nil
}
