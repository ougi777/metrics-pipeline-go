// Package config 从环境变量加载并校验进程配置。
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const defaultShutdownTimeout = 15 * time.Second

// Config 包含所有进程共享的运行时配置。
type Config struct {
	ServiceName     string
	InstanceID      string
	HTTPAddr        string
	AdminAddr       string
	ShutdownTimeout time.Duration
	LogLevel        string
}

// Load 先读取可选的本地 .env 文件，再从环境变量解析配置。
// 已存在的环境变量拥有更高优先级。
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

// Validate 在进程启动前校验公共配置。
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
