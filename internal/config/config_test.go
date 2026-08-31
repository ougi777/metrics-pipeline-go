package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func TestLoadFromEnvironment(t *testing.T) {
	t.Setenv("SERVICE_NAME", "api")
	t.Setenv("INSTANCE_ID", "local")
	t.Setenv("HTTP_ADDR", ":8080")
	t.Setenv("ADMIN_ADDR", ":8081")
	t.Setenv("SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("DATABASE_URL", "postgres://metrics:metrics@localhost:5432/metrics")
	t.Setenv("AMQP_URL", "amqp://metrics:metrics@localhost:5672/")
	t.Setenv("AMQP_PUBLISHERS", "2")
	t.Setenv("AMQP_WRITE_TIMEOUT", "2s")
	t.Setenv("AMQP_CONFIRM_TIMEOUT", "3s")
	t.Setenv("AMQP_PUBLISH_MAX_ATTEMPTS", "4")
	t.Setenv("AMQP_PUBLISH_INITIAL_BACKOFF", "25ms")
	t.Setenv("AMQP_PUBLISH_MAX_BACKOFF", "250ms")

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
	if cfg.AMQPURL != "amqp://metrics:metrics@localhost:5672/" {
		t.Errorf("AMQPURL = %q, want configured URL", cfg.AMQPURL)
	}
	if cfg.DatabaseURL != "postgres://metrics:metrics@localhost:5432/metrics" {
		t.Errorf("DatabaseURL = %q, want configured URL", cfg.DatabaseURL)
	}
	if cfg.AMQPPublishers != 2 {
		t.Errorf("AMQPPublishers = %d, want 2", cfg.AMQPPublishers)
	}
	if cfg.AMQPWriteTimeout != 2*time.Second {
		t.Errorf("AMQPWriteTimeout = %s, want 2s", cfg.AMQPWriteTimeout)
	}
	if cfg.AMQPConfirmTimeout != 3*time.Second {
		t.Errorf("AMQPConfirmTimeout = %s, want 3s", cfg.AMQPConfirmTimeout)
	}
	if cfg.AMQPPublishMaxAttempts != 4 {
		t.Errorf("AMQPPublishMaxAttempts = %d, want 4", cfg.AMQPPublishMaxAttempts)
	}
	if cfg.AMQPPublishInitialBackoff != 25*time.Millisecond {
		t.Errorf("AMQPPublishInitialBackoff = %s, want 25ms", cfg.AMQPPublishInitialBackoff)
	}
	if cfg.AMQPPublishMaxBackoff != 250*time.Millisecond {
		t.Errorf("AMQPPublishMaxBackoff = %s, want 250ms", cfg.AMQPPublishMaxBackoff)
	}
	if cfg.RetentionWindow != 168*time.Hour {
		t.Errorf("RetentionWindow = %s, want 168h", cfg.RetentionWindow)
	}
	if cfg.PartitionMaintenanceInterval != time.Hour {
		t.Errorf("PartitionMaintenanceInterval = %s, want 1h", cfg.PartitionMaintenanceInterval)
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
	t.Setenv("SERVICE_NAME", "sim")
	t.Setenv("SHUTDOWN_TIMEOUT", "later")

	if _, err := Load("api"); err == nil {
		t.Fatal("Load() error = nil, want invalid duration error")
	}
}

func TestLoadRejectsRetentionWindowOutsideSevenDays(t *testing.T) {
	t.Setenv("SERVICE_NAME", "sim")
	t.Setenv("RETENTION_WINDOW", "24h")

	if _, err := Load("sim"); err == nil {
		t.Fatal("Load() error = nil, want invalid retention window error")
	}
}

func TestValidateRejectsInvalidLogLevel(t *testing.T) {
	cfg := Config{
		ServiceName:                  "api",
		InstanceID:                   "local",
		HTTPAddr:                     ":8080",
		AdminAddr:                    ":8081",
		ShutdownTimeout:              time.Second,
		LogLevel:                     "verbose",
		AMQPURL:                      "amqp://metrics:metrics@localhost:5672/",
		AMQPPublishers:               1,
		AMQPWriteTimeout:             time.Second,
		AMQPConfirmTimeout:           time.Second,
		AMQPPublishMaxAttempts:       3,
		AMQPPublishInitialBackoff:    time.Millisecond,
		AMQPPublishMaxBackoff:        time.Second,
		RetentionWindow:              168 * time.Hour,
		PartitionMaintenanceInterval: time.Hour,
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want invalid log level error")
	}
}

func TestExampleEnvironmentLoadsForAPI(t *testing.T) {
	values, err := godotenv.Read(filepath.Join("..", "..", ".env.example"))
	if err != nil {
		t.Fatalf("Read(.env.example) error = %v", err)
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
	t.Setenv("SERVICE_NAME", "api")

	cfg, err := Load("api")
	if err != nil {
		t.Fatalf("Load() with .env.example error = %v", err)
	}
	if cfg.AMQPURL != "amqp://metrics:metrics@localhost:5672/" {
		t.Fatalf("AMQPURL = %q, want local RabbitMQ URL", cfg.AMQPURL)
	}
}

func TestLoadRejectsMissingAMQPURLForAPI(t *testing.T) {
	t.Setenv("SERVICE_NAME", "api")
	t.Setenv("INSTANCE_ID", "local")
	t.Setenv("HTTP_ADDR", ":8080")
	t.Setenv("ADMIN_ADDR", ":8081")
	t.Setenv("SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("LOG_LEVEL", "info")
	unsetEnv(t, "AMQP_URL")

	if _, err := Load("api"); err == nil {
		t.Fatal("Load() error = nil, want missing AMQP_URL error")
	}
}

func TestLoadRejectsMissingDatabaseURLForAPI(t *testing.T) {
	t.Setenv("SERVICE_NAME", "api")
	t.Setenv("INSTANCE_ID", "local")
	t.Setenv("HTTP_ADDR", ":8080")
	t.Setenv("ADMIN_ADDR", ":8081")
	t.Setenv("SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("AMQP_URL", "amqp://metrics:metrics@localhost:5672/")
	unsetEnv(t, "DATABASE_URL")

	if _, err := Load("api"); err == nil {
		t.Fatal("Load() error = nil, want missing DATABASE_URL error")
	}
}

func TestLoadAllowsMissingAMQPURLForSimulator(t *testing.T) {
	t.Setenv("SERVICE_NAME", "sim")
	t.Setenv("INSTANCE_ID", "local")
	t.Setenv("HTTP_ADDR", ":8080")
	t.Setenv("ADMIN_ADDR", ":8081")
	t.Setenv("SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("LOG_LEVEL", "info")
	unsetEnv(t, "AMQP_URL")

	if _, err := Load("sim"); err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
}

func TestLoadRejectsMissingDatabaseURLForWorker(t *testing.T) {
	t.Setenv("SERVICE_NAME", "worker")
	t.Setenv("INSTANCE_ID", "local")
	t.Setenv("HTTP_ADDR", ":8090")
	t.Setenv("ADMIN_ADDR", ":8091")
	t.Setenv("SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("AMQP_URL", "amqp://metrics:metrics@localhost:5672/")
	unsetEnv(t, "DATABASE_URL")

	if _, err := Load("worker"); err == nil {
		t.Fatal("Load() error = nil, want missing DATABASE_URL error")
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
