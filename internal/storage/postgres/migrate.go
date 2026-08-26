package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var migrationFilePattern = regexp.MustCompile(`^(\d+)_([a-z0-9][a-z0-9_-]*)\.sql$`)

const migrationLockName = "metrics-pipeline-migrations"

// Migration 描述一条有序的 SQL 结构迁移。
type Migration struct {
	Version  int64
	Name     string
	Filename string
	SQL      string
	Checksum string
}

// Migrator 应用版本化结构迁移并初始化日分区。
type Migrator struct {
	connection *pgx.Conn
	migrations fs.FS
	logger     *slog.Logger
}

// NewMigrator 使用指定数据库连接和迁移文件创建迁移执行器。
func NewMigrator(connection *pgx.Conn, migrations fs.FS, logger *slog.Logger) *Migrator {
	return &Migrator{connection: connection, migrations: migrations, logger: logger}
}

// Run 串行执行迁移、校验文件摘要并应用待执行版本。
func (m *Migrator) Run(ctx context.Context) (int, error) {
	if _, err := m.connection.Exec(
		ctx,
		"SELECT pg_advisory_lock(hashtextextended($1, 0))",
		migrationLockName,
	); err != nil {
		return 0, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := m.connection.Exec(
			unlockCtx,
			"SELECT pg_advisory_unlock(hashtextextended($1, 0))",
			migrationLockName,
		); err != nil {
			m.logger.Error("release migration lock", slog.Any("error", err))
		}
	}()

	if err := m.ensureVersionTable(ctx); err != nil {
		return 0, err
	}

	migrations, err := DiscoverMigrations(m.migrations)
	if err != nil {
		return 0, err
	}

	applied := 0
	for _, migration := range migrations {
		alreadyApplied, err := m.isApplied(ctx, migration)
		if err != nil {
			return applied, err
		}
		if alreadyApplied {
			continue
		}

		if err := m.apply(ctx, migration); err != nil {
			return applied, err
		}
		applied++
		m.logger.Info(
			"migration applied",
			slog.Int64("version", migration.Version),
			slog.String("name", migration.Name),
		)
	}

	return applied, nil
}

// EnsureDailyPartitions 围绕指定基准日期创建 UTC 日分区。
func (m *Migrator) EnsureDailyPartitions(
	ctx context.Context,
	referenceDay time.Time,
	pastDays int,
	futureDays int,
) error {
	if pastDays < 0 || futureDays < 0 || pastDays > 366 || futureDays > 366 {
		return fmt.Errorf("partition day ranges must be between 0 and 366")
	}

	day := referenceDay.UTC().Format(time.DateOnly)
	if _, err := m.connection.Exec(
		ctx,
		"SELECT ensure_metric_daily_partitions($1::date, $2::integer, $3::integer)",
		day,
		pastDays,
		futureDays,
	); err != nil {
		return fmt.Errorf("ensure daily partitions: %w", err)
	}

	return nil
}

// DiscoverMigrations 读取、校验、计算摘要并排序 SQL 迁移文件。
func DiscoverMigrations(migrations fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(migrations, ".")
	if err != nil {
		return nil, fmt.Errorf("read migration directory: %w", err)
	}

	result := make([]Migration, 0, len(entries))
	versions := make(map[int64]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := filepath.ToSlash(entry.Name())
		if !strings.HasSuffix(filename, ".sql") {
			continue
		}

		matches := migrationFilePattern.FindStringSubmatch(filename)
		if matches == nil {
			return nil, fmt.Errorf("invalid migration filename %q", filename)
		}

		version, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("invalid migration version in %q", filename)
		}
		if previous, exists := versions[version]; exists {
			return nil, fmt.Errorf(
				"duplicate migration version %d in %q and %q",
				version,
				previous,
				filename,
			)
		}

		contents, err := fs.ReadFile(migrations, filename)
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", filename, err)
		}
		checksum := sha256.Sum256(contents)
		versions[version] = filename
		result = append(result, Migration{
			Version:  version,
			Name:     matches[2],
			Filename: filename,
			SQL:      string(contents),
			Checksum: hex.EncodeToString(checksum[:]),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Version < result[j].Version
	})
	if len(result) == 0 {
		return nil, errors.New("migration directory contains no SQL migrations")
	}

	return result, nil
}

func (m *Migrator) ensureVersionTable(ctx context.Context) error {
	const statement = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version bigint PRIMARY KEY,
    name text NOT NULL,
    checksum char(64) NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`
	if _, err := m.connection.Exec(ctx, statement); err != nil {
		return fmt.Errorf("create schema migrations table: %w", err)
	}

	return nil
}

func (m *Migrator) isApplied(ctx context.Context, migration Migration) (bool, error) {
	var checksum string
	err := m.connection.QueryRow(
		ctx,
		"SELECT checksum FROM schema_migrations WHERE version = $1",
		migration.Version,
	).Scan(&checksum)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read migration version %d: %w", migration.Version, err)
	}
	if checksum != migration.Checksum {
		return false, fmt.Errorf(
			"migration version %d checksum changed: database=%s file=%s",
			migration.Version,
			checksum,
			migration.Checksum,
		)
	}

	return true, nil
}

func (m *Migrator) apply(ctx context.Context, migration Migration) error {
	tx, err := m.connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration version %d: %w", migration.Version, err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	if _, err := tx.Exec(ctx, migration.SQL); err != nil {
		return fmt.Errorf("execute migration %q: %w", migration.Filename, err)
	}
	if _, err := tx.Exec(
		ctx,
		"INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
		migration.Version,
		migration.Name,
		migration.Checksum,
	); err != nil {
		return fmt.Errorf("record migration version %d: %w", migration.Version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration version %d: %w", migration.Version, err)
	}

	return nil
}
