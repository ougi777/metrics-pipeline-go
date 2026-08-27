package app

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

func TestWaitForCancelStopsWhenContextIsCanceled(t *testing.T) {
	setValidEnvironment(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if exitCode := WaitForCancel(ctx, logger); exitCode != 0 {
		t.Fatalf("WaitForCancel() exit code = %d, want 0", exitCode)
	}
}

func TestBootstrapReturnsRuntimeForValidConfiguration(t *testing.T) {
	setValidEnvironment(t)

	runtime, exitCode := Bootstrap("api")
	if exitCode != 0 {
		t.Fatalf("Bootstrap() exit code = %d, want 0", exitCode)
	}
	if runtime.Config.ServiceName != "api" {
		t.Fatalf("ServiceName = %q, want api", runtime.Config.ServiceName)
	}
	if runtime.Logger == nil {
		t.Fatal("Logger is nil")
	}
}

func TestBootstrapReturnsFailureForInvalidConfiguration(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("SHUTDOWN_TIMEOUT", "invalid")

	_, exitCode := Bootstrap("api")
	if exitCode != 1 {
		t.Fatalf("Bootstrap() exit code = %d, want 1", exitCode)
	}
}

func setValidEnvironment(t *testing.T) {
	t.Helper()

	t.Setenv("SERVICE_NAME", "api")
	t.Setenv("INSTANCE_ID", "test-1")
	t.Setenv("HTTP_ADDR", ":8080")
	t.Setenv("ADMIN_ADDR", ":8081")
	t.Setenv("SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("LOG_LEVEL", "info")
}
