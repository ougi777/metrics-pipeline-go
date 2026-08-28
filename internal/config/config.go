// Package config 从环境变量加载并校验进程配置。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const defaultShutdownTimeout = 15 * time.Second
const defaultAMQPPublishers = 1
const defaultAMQPWriteTimeout = 5 * time.Second
const defaultAMQPConfirmTimeout = 5 * time.Second
const defaultAMQPPublishMaxAttempts = 3
const defaultAMQPPublishInitialBackoff = 100 * time.Millisecond
const defaultAMQPPublishMaxBackoff = time.Second

// Config 包含所有进程共享的运行时配置。
type Config struct {
	ServiceName               string
	InstanceID                string
	HTTPAddr                  string
	AdminAddr                 string
	ShutdownTimeout           time.Duration
	LogLevel                  string
	DatabaseURL               string
	AMQPURL                   string
	AMQPPublishers            int
	AMQPWriteTimeout          time.Duration
	AMQPConfirmTimeout        time.Duration
	AMQPPublishMaxAttempts    int
	AMQPPublishInitialBackoff time.Duration
	AMQPPublishMaxBackoff     time.Duration
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
	amqpWriteTimeout, err := durationEnv("AMQP_WRITE_TIMEOUT", defaultAMQPWriteTimeout)
	if err != nil {
		return Config{}, err
	}
	amqpConfirmTimeout, err := durationEnv("AMQP_CONFIRM_TIMEOUT", defaultAMQPConfirmTimeout)
	if err != nil {
		return Config{}, err
	}
	amqpPublishInitialBackoff, err := durationEnv("AMQP_PUBLISH_INITIAL_BACKOFF", defaultAMQPPublishInitialBackoff)
	if err != nil {
		return Config{}, err
	}
	amqpPublishMaxBackoff, err := durationEnv("AMQP_PUBLISH_MAX_BACKOFF", defaultAMQPPublishMaxBackoff)
	if err != nil {
		return Config{}, err
	}
	amqpPublishers, err := intEnv("AMQP_PUBLISHERS", defaultAMQPPublishers)
	if err != nil {
		return Config{}, err
	}
	amqpPublishMaxAttempts, err := intEnv("AMQP_PUBLISH_MAX_ATTEMPTS", defaultAMQPPublishMaxAttempts)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		ServiceName:               stringEnv("SERVICE_NAME", defaultServiceName),
		InstanceID:                stringEnv("INSTANCE_ID", "local"),
		HTTPAddr:                  stringEnv("HTTP_ADDR", ":8080"),
		AdminAddr:                 stringEnv("ADMIN_ADDR", ":8081"),
		ShutdownTimeout:           shutdownTimeout,
		LogLevel:                  strings.ToLower(stringEnv("LOG_LEVEL", "info")),
		DatabaseURL:               stringEnv("DATABASE_URL", ""),
		AMQPURL:                   stringEnv("AMQP_URL", ""),
		AMQPPublishers:            amqpPublishers,
		AMQPWriteTimeout:          amqpWriteTimeout,
		AMQPConfirmTimeout:        amqpConfirmTimeout,
		AMQPPublishMaxAttempts:    amqpPublishMaxAttempts,
		AMQPPublishInitialBackoff: amqpPublishInitialBackoff,
		AMQPPublishMaxBackoff:     amqpPublishMaxBackoff,
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
	if requiresAMQP(c.ServiceName) && strings.TrimSpace(c.AMQPURL) == "" {
		return fmt.Errorf("AMQP_URL must not be empty for %s", c.ServiceName)
	}
	if requiresDatabase(c.ServiceName) && strings.TrimSpace(c.DatabaseURL) == "" {
		return fmt.Errorf("DATABASE_URL must not be empty for %s", c.ServiceName)
	}
	if c.AMQPPublishers <= 0 {
		return fmt.Errorf("AMQP_PUBLISHERS must be greater than zero")
	}
	if c.AMQPWriteTimeout <= 0 {
		return fmt.Errorf("AMQP_WRITE_TIMEOUT must be greater than zero")
	}
	if c.AMQPConfirmTimeout <= 0 {
		return fmt.Errorf("AMQP_CONFIRM_TIMEOUT must be greater than zero")
	}
	if c.AMQPPublishMaxAttempts <= 0 {
		return fmt.Errorf("AMQP_PUBLISH_MAX_ATTEMPTS must be greater than zero")
	}
	if c.AMQPPublishInitialBackoff <= 0 {
		return fmt.Errorf("AMQP_PUBLISH_INITIAL_BACKOFF must be greater than zero")
	}
	if c.AMQPPublishMaxBackoff < c.AMQPPublishInitialBackoff {
		return fmt.Errorf("AMQP_PUBLISH_MAX_BACKOFF must be greater than or equal to AMQP_PUBLISH_INITIAL_BACKOFF")
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

func intEnv(name string, fallback int) (int, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}

	integer, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}

	return integer, nil
}

func requiresAMQP(serviceName string) bool {
	switch serviceName {
	case "api", "worker":
		return true
	default:
		return false
	}
}

func requiresDatabase(serviceName string) bool {
	return serviceName == "worker"
}
