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
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
)

var taskPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

type Config struct {
	Endpoint   string
	Tasks      int
	Rate       float64
	Duration   time.Duration
	BatchSize  int
	TaskPrefix string
	EvalLoss   bool
	EvalEvery  int
	GPUUtil    bool
	GPUMem     bool
	Throughput bool
	Seed       int64
	Client     *http.Client
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
	return nil
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
	if err := cfg.Validate(); err != nil {
		return err
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	var wg sync.WaitGroup
	errCh := make(chan error, cfg.Tasks)
	//一个task启动一个协程
	for task := 0; task < cfg.Tasks; task++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			id := fmt.Sprintf("%s-%04d", cfg.TaskPrefix, index+1)
			gen := NewGenerator(id, time.Now(), cfg, cfg.Seed+int64(index))
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
				//post到api接口
				if err := postBatch(ctx, client, cfg.Endpoint, batch); err != nil {
					select {
					case errCh <- fmt.Errorf("task %s: %w", id, err):
					default:
					}
					return
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
		return err
	default:
		return nil
	}
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
