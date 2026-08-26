package app

import (
	"context"
	"testing"
)

func TestRunServiceStopsWhenContextIsCanceled(t *testing.T) {
	setValidEnvironment(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if exitCode := RunService(ctx, "api"); exitCode != 0 {
		t.Fatalf("RunService() exit code = %d, want 0", exitCode)
	}
}

func TestRunServiceReturnsFailureForInvalidConfiguration(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("SHUTDOWN_TIMEOUT", "invalid")

	if exitCode := RunService(context.Background(), "api"); exitCode != 1 {
		t.Fatalf("RunService() exit code = %d, want 1", exitCode)
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
