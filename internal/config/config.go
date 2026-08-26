// Package config loads and validates process configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const defaultShutdownTimeout = 15 * time.Second

// Config contains the common runtime settings shared by every process.
type Config struct {
	ServiceName     string
	InstanceID      string
	HTTPAddr        string
	AdminAddr       string
	ShutdownTimeout time.Duration
	LogLevel        string
}

// Load reads an optional local .env file, then resolves configuration from
// environment variables. Existing environment variables take precedence.
func Load(defaultServiceName string) (Config, error) {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return Config{}, fmt.Errorf("load .env: %w", err)
	}

	shutdownTimeout, err := durationEnv("SHUTDOWN_TIMEOUT", defaultShutdownTimeout)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		ServiceName:     stringEnv("SERVICE_NAME", defaultServiceName),
		InstanceID:      stringEnv("INSTANCE_ID", "local"),
		HTTPAddr:        stringEnv("HTTP_ADDR", ":8080"),
		AdminAddr:       stringEnv("ADMIN_ADDR", ":8081"),
		ShutdownTimeout: shutdownTimeout,
		LogLevel:        strings.ToLower(stringEnv("LOG_LEVEL", "info")),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Validate verifies the common settings before a process starts.
func (c Config) Validate() error {
	if strings.TrimSpace(c.ServiceName) == "" {
		return fmt.Errorf("SERVICE_NAME must not be empty")
	}
	if strings.TrimSpace(c.InstanceID) == "" {
		return fmt.Errorf("INSTANCE_ID must not be empty")
	}
	if strings.TrimSpace(c.HTTPAddr) == "" {
		return fmt.Errorf("HTTP_ADDR must not be empty")
	}
	if strings.TrimSpace(c.AdminAddr) == "" {
		return fmt.Errorf("ADMIN_ADDR must not be empty")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be greater than zero")
	}

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
		return nil
	default:
		return fmt.Errorf("LOG_LEVEL must be one of debug, info, warn, or error")
	}
}

func stringEnv(name, fallback string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}

	return value
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}

	return duration, nil
}
