package postgres

import (
	"context"
	"testing"
	"testing/fstest"
	"time"
)

func TestDiscoverMigrationsSortsAndChecksumsFiles(t *testing.T) {
	files := fstest.MapFS{
		"000002_second.sql": {Data: []byte("SELECT 2;")},
		"000001_first.sql":  {Data: []byte("SELECT 1;")},
		"README.md":         {Data: []byte("ignored")},
	}

	migrations, err := DiscoverMigrations(files)
	if err != nil {
		t.Fatalf("DiscoverMigrations() error = %v", err)
	}
	if len(migrations) != 2 {
		t.Fatalf("migration count = %d, want 2", len(migrations))
	}
	if migrations[0].Version != 1 || migrations[1].Version != 2 {
		t.Fatalf(
			"migration versions = [%d, %d], want [1, 2]",
			migrations[0].Version,
			migrations[1].Version,
		)
	}
	if migrations[0].Checksum == "" {
		t.Fatal("migration checksum is empty")
	}
}

func TestEnsureDailyPartitionsRejectsInvalidRange(t *testing.T) {
	migrator := NewMigrator(nil, nil, nil)

	err := migrator.EnsureDailyPartitions(context.Background(), time.Now(), -1, 2)
	if err == nil {
		t.Fatal("EnsureDailyPartitions() error = nil, want invalid range error")
	}
}

func TestDiscoverMigrationsRejectsDuplicateVersion(t *testing.T) {
	files := fstest.MapFS{
		"000001_first.sql":  {Data: []byte("SELECT 1;")},
		"000001_second.sql": {Data: []byte("SELECT 2;")},
	}

	if _, err := DiscoverMigrations(files); err == nil {
		t.Fatal("DiscoverMigrations() error = nil, want duplicate version error")
	}
}

func TestDiscoverMigrationsRejectsInvalidSQLFilename(t *testing.T) {
	files := fstest.MapFS{
		"initial.sql": {Data: []byte("SELECT 1;")},
	}

	if _, err := DiscoverMigrations(files); err == nil {
		t.Fatal("DiscoverMigrations() error = nil, want invalid filename error")
	}
}
