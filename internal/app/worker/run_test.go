package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/ougi777/metrics-pipeline-go/internal/config"
)

func TestRunServiceReturnsSuccessForCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if exitCode := runService(ctx, config.Config{}, logger); exitCode != 0 {
		t.Fatalf("runService() exit code = %d, want 0", exitCode)
	}
}
