// Package worker 装配 worker 进程的运行时依赖和生命周期。
package worker

import (
	"context"

	baseapp "github.com/ougi777/metrics-pipeline-go/internal/app"
)

// Run 启动 worker 进程。
func Run(ctx context.Context) int {
	runtime, exitCode := baseapp.Bootstrap("worker")
	if exitCode != 0 {
		return exitCode
	}

	return baseapp.WaitForCancel(ctx, runtime.Logger)
}
