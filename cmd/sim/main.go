package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/simulator"
)

func main() {
	var cfg simulator.Config
	flag.StringVar(&cfg.Endpoint, "endpoint", "http://localhost:8080/api/v1/ingest/metrics", "metric ingest endpoint")
	flag.IntVar(&cfg.Tasks, "tasks", 1, "number of concurrent tasks")
	flag.Float64Var(&cfg.Rate, "rate", 1, "samples per second per task")
	flag.DurationVar(&cfg.Duration, "duration", time.Minute, "simulation duration")
	flag.IntVar(&cfg.BatchSize, "batch-size", 1, "samples per request")
	flag.StringVar(&cfg.TaskPrefix, "task-prefix", "sim", "task id prefix")
	flag.BoolVar(&cfg.EvalLoss, "eval-loss", false, "enable sparse eval_loss")
	flag.IntVar(&cfg.EvalEvery, "eval-every", 10, "emit eval_loss every N steps")
	flag.BoolVar(&cfg.GPUUtil, "gpu-util", false, "enable gpu_util")
	flag.BoolVar(&cfg.GPUMem, "gpu-mem", false, "enable gpu_mem")
	flag.BoolVar(&cfg.Throughput, "throughput", false, "enable throughput in tokens/s")
	flag.Int64Var(&cfg.Seed, "seed", 1, "random seed")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := simulator.Run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
