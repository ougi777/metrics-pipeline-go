// Package logging creates the structured logger shared by all processes.
package logging

import (
	"io"
	"log/slog"
)

// New returns a JSON logger enriched with process identity fields.
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
