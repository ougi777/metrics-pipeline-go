package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"github.com/ougi777/metrics-pipeline-go/internal/logging"
	postgresstore "github.com/ougi777/metrics-pipeline-go/internal/storage/postgres"
)

const (
	defaultDatabaseURL         = "postgres://metrics:metrics@localhost:5432/metrics?sslmode=disable"
	defaultMigrationsDir       = "migrations"
	defaultMigrationTimeout    = time.Minute
	defaultPartitionPastDays   = 8
	defaultPartitionFutureDays = 2
)

func main() {
	os.Exit(run())
}

func run() int {
	logger := logging.New(os.Stdout, "migrate", "migrate-local-1", "info")
	configuration, err := loadConfiguration()
	if err != nil {
		logger.Error("migration configuration failed", slog.Any("error", err))
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), configuration.timeout)
	defer cancel()

	connection, err := pgx.Connect(ctx, configuration.databaseURL)
	if err != nil {
		logger.Error("database connection failed", slog.Any("error", err))
		return 1
	}
	defer func() {
		if closeErr := connection.Close(context.Background()); closeErr != nil {
			logger.Error("close database connection", slog.Any("error", closeErr))
		}
	}()

	migrations := os.DirFS(configuration.migrationsDir)
	migrator := postgresstore.NewMigrator(connection, migrations, logger)
	applied, err := migrator.Run(ctx)
	if err != nil {
		logger.Error("database migration failed", slog.Any("error", err))
		return 1
	}
	if err := migrator.EnsureDailyPartitions(
		ctx,
		time.Now().UTC(),
		configuration.partitionPastDays,
		configuration.partitionFutureDays,
	); err != nil {
		logger.Error("partition initialization failed", slog.Any("error", err))
		return 1
	}

	logger.Info(
		"database migration completed",
		slog.Int("applied", applied),
		slog.Int("partition_past_days", configuration.partitionPastDays),
		slog.Int("partition_future_days", configuration.partitionFutureDays),
	)

	return 0
}

type migrationConfiguration struct {
	databaseURL         string
	migrationsDir       string
	timeout             time.Duration
	partitionPastDays   int
	partitionFutureDays int
}

func loadConfiguration() (migrationConfiguration, error) {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return migrationConfiguration{}, fmt.Errorf("load .env: %w", err)
	}

	timeout, err := durationEnvironment("MIGRATION_TIMEOUT", defaultMigrationTimeout)
	if err != nil {
		return migrationConfiguration{}, err
	}
	pastDays, err := integerEnvironment("PARTITION_PAST_DAYS", defaultPartitionPastDays)
	if err != nil {
		return migrationConfiguration{}, err
	}
	futureDays, err := integerEnvironment("PARTITION_FUTURE_DAYS", defaultPartitionFutureDays)
	if err != nil {
		return migrationConfiguration{}, err
	}
	if pastDays < 0 || futureDays < 0 || pastDays > 366 || futureDays > 366 {
		return migrationConfiguration{}, fmt.Errorf("partition day ranges must be between 0 and 366")
	}

	databaseURL := strings.TrimSpace(stringEnvironment("DATABASE_URL", defaultDatabaseURL))
	if databaseURL == "" {
		return migrationConfiguration{}, fmt.Errorf("DATABASE_URL must not be empty")
	}
	migrationsDir := strings.TrimSpace(stringEnvironment("MIGRATIONS_DIR", defaultMigrationsDir))
	if migrationsDir == "" {
		return migrationConfiguration{}, fmt.Errorf("MIGRATIONS_DIR must not be empty")
	}

	return migrationConfiguration{
		databaseURL:         databaseURL,
		migrationsDir:       migrationsDir,
		timeout:             timeout,
		partitionPastDays:   pastDays,
		partitionFutureDays: futureDays,
	}, nil
}

func stringEnvironment(name, fallback string) string {
	value, exists := os.LookupEnv(name)
	if !exists {
		return fallback
	}

	return value
}

func durationEnvironment(name string, fallback time.Duration) (time.Duration, error) {
	value, exists := os.LookupEnv(name)
	if !exists {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}

	return parsed, nil
}

func integerEnvironment(name string, fallback int) (int, error) {
	value, exists := os.LookupEnv(name)
	if !exists {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}

	return parsed, nil
}
