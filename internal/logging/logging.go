// Package logging 创建所有进程共享的结构化日志器。
package logging

import (
	"io"
	"log/slog"
)

// New 返回带进程标识字段的 JSON 日志器。
func New(output io.Writer, serviceName, instanceID, level string) *slog.Logger {
	var slogLevel slog.Level
	switch level {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{Level: slogLevel})
	return slog.New(handler).With(
		slog.String("service", serviceName),
		slog.String("instance_id", instanceID),
	)
}
