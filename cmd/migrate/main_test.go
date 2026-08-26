package main

import (
	"testing"
	"time"
)

func TestLoadConfigurationFromEnvironment(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("MIGRATIONS_DIR", "test-migrations")
	t.Setenv("MIGRATION_TIMEOUT", "30s")
	t.Setenv("PARTITION_PAST_DAYS", "7")
	t.Setenv("PARTITION_FUTURE_DAYS", "3")

	configuration, err := loadConfiguration()
	if err != nil {
		t.Fatalf("loadConfiguration() error = %v", err)
	}
	if configuration.databaseURL != "postgres://test" {
		t.Errorf("databaseURL = %q, want postgres://test", configuration.databaseURL)
	}
	if configuration.timeout != 30*time.Second {
		t.Errorf("timeout = %s, want 30s", configuration.timeout)
	}
	if configuration.partitionPastDays != 7 || configuration.partitionFutureDays != 3 {
		t.Errorf(
			"partition range = [%d, %d], want [7, 3]",
			configuration.partitionPastDays,
			configuration.partitionFutureDays,
		)
	}
}

func TestLoadConfigurationRejectsInvalidPartitionRange(t *testing.T) {
	t.Setenv("PARTITION_PAST_DAYS", "-1")

	if _, err := loadConfiguration(); err == nil {
		t.Fatal("loadConfiguration() error = nil, want invalid partition range error")
	}
}

func TestLoadConfigurationRejectsEmptyMigrationDirectory(t *testing.T) {
	t.Setenv("MIGRATIONS_DIR", " ")

	if _, err := loadConfiguration(); err == nil {
		t.Fatal("loadConfiguration() error = nil, want empty migration directory error")
	}
}
