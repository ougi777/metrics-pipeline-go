package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadFromEnvironment(t *testing.T) {
	t.Setenv("SERVICE_NAME", "api")
	t.Setenv("INSTANCE_ID", "local")
	t.Setenv("HTTP_ADDR", ":8080")
	t.Setenv("ADMIN_ADDR", ":8081")
	t.Setenv("SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("LOG_LEVEL", "info")

	cfg, err := Load("fallback")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.ServiceName != "api" {
		t.Errorf("ServiceName = %q, want api", cfg.ServiceName)
	}
	if cfg.ShutdownTimeout != 15*time.Second {
		t.Errorf("ShutdownTimeout = %s, want 15s", cfg.ShutdownTimeout)
	}
}

func TestLoadDotEnvAndPreservesExistingEnvironment(t *testing.T) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}

	tempDirectory, err := os.MkdirTemp("", "metrics-pipeline-config-test-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	dotEnvPath := filepath.Join(tempDirectory, ".env")
	t.Cleanup(func() {
		if removeErr := os.Remove(dotEnvPath); removeErr != nil && !os.IsNotExist(removeErr) {
			t.Errorf("remove .env: %v", removeErr)
		}
		if removeErr := os.Remove(tempDirectory); removeErr != nil && !os.IsNotExist(removeErr) {
			t.Errorf("remove temporary directory: %v", removeErr)
		}
	})
	t.Cleanup(func() {
		if chdirErr := os.Chdir(workingDirectory); chdirErr != nil {
			t.Errorf("restore working directory: %v", chdirErr)
		}
	})

	dotEnv := []byte("SERVICE_NAME=from-dotenv\nINSTANCE_ID=dotenv-1\n")
	if err := os.WriteFile(dotEnvPath, dotEnv, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.Chdir(tempDirectory); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}

	unsetEnv(t, "SERVICE_NAME")
	t.Setenv("INSTANCE_ID", "from-environment")

	cfg, err := Load("fallback")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ServiceName != "from-dotenv" {
		t.Errorf("ServiceName = %q, want from-dotenv", cfg.ServiceName)
	}
	if cfg.InstanceID != "from-environment" {
		t.Errorf("InstanceID = %q, want from-environment", cfg.InstanceID)
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "later")

	if _, err := Load("api"); err == nil {
		t.Fatal("Load() error = nil, want invalid duration error")
	}
}

func TestValidateRejectsInvalidLogLevel(t *testing.T) {
	cfg := Config{
		ServiceName:     "api",
		InstanceID:      "local",
		HTTPAddr:        ":8080",
		AdminAddr:       ":8081",
		ShutdownTimeout: time.Second,
		LogLevel:        "verbose",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want invalid log level error")
	}
}

func unsetEnv(t *testing.T, name string) {
	t.Helper()

	value, exists := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatalf("Unsetenv(%q) error = %v", name, err)
	}
	t.Cleanup(func() {
		if exists {
			if err := os.Setenv(name, value); err != nil {
				t.Errorf("restore %s: %v", name, err)
			}
			return
		}
		if err := os.Unsetenv(name); err != nil {
			t.Errorf("clear %s: %v", name, err)
		}
	})
}
