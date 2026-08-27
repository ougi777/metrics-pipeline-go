// Package sim 装配模拟器进程。
package sim

import baseapp "github.com/ougi777/metrics-pipeline-go/internal/app"

// Run 初始化模拟器进程。
func Run() int {
	_, exitCode := baseapp.Bootstrap("sim")
	return exitCode
}
