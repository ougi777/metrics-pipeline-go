package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMaintainRetentionUsesUTCWindowAndScansResult(t *testing.T) {
	cutoff := time.Date(2026, time.August, 28, 12, 34, 56, 0, time.FixedZone("offset", 9*60*60))
	executor := &retentionExecutorStub{row: retentionRow{values: []int{2, 3, 4, 5, 6}}}

	result, err := MaintainRetention(context.Background(), executor, cutoff, 100)
	if err != nil {
		t.Fatalf("MaintainRetention() error = %v", err)
	}
	if result != (RetentionResult{
		PointPartitionsDropped: 2,
		EventPartitionsDropped: 3,
		MetricPointsDeleted:    4,
		MetricEventsDeleted:    5,
		MetricOutboxDeleted:    6,
	}) {
		t.Fatalf("MaintainRetention() result = %#v", result)
	}
	if executor.sql != maintainMetricRetentionSQL {
		t.Fatal("MaintainRetention() used an unexpected SQL statement")
	}
	if len(executor.args) != 2 {
		t.Fatalf("MaintainRetention() argument count = %d, want 2", len(executor.args))
	}
	if got, ok := executor.args[0].(time.Time); !ok || !got.Equal(cutoff.UTC()) || got.Location() != time.UTC {
		t.Fatalf("cutoff argument = %#v, want UTC %s", executor.args[0], cutoff.UTC())
	}
	if got := executor.args[1]; got != 100 {
		t.Fatalf("batch size argument = %#v, want 100", got)
	}
}

func TestMaintainRetentionRejectsInvalidInput(t *testing.T) {
	validCutoff := time.Now()
	tests := []struct {
		name     string
		executor retentionExecutor
		cutoff   time.Time
		batch    int
		want     string
	}{
		{name: "missing executor", cutoff: validCutoff, batch: 1, want: "retention executor"},
		{name: "missing cutoff", executor: &retentionExecutorStub{}, batch: 1, want: "retention cutoff"},
		{name: "invalid batch", executor: &retentionExecutorStub{}, cutoff: validCutoff, batch: 0, want: "batch size"},
		{name: "oversized batch", executor: &retentionExecutorStub{}, cutoff: validCutoff, batch: 100001, want: "batch size"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := MaintainRetention(context.Background(), test.executor, test.cutoff, test.batch)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("MaintainRetention() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestMaintainRetentionWrapsDatabaseErrors(t *testing.T) {
	executor := &retentionExecutorStub{row: retentionRow{err: errors.New("database unavailable")}}
	_, err := MaintainRetention(context.Background(), executor, time.Now(), 1)
	if err == nil || !strings.Contains(err.Error(), "maintain metric retention") {
		t.Fatalf("MaintainRetention() error = %v", err)
	}
}

type retentionExecutorStub struct {
	sql  string
	args []any
	row  pgx.Row
}

func (s *retentionExecutorStub) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	s.sql = sql
	s.args = args
	return s.row
}

type retentionRow struct {
	values []int
	err    error
}

func (r retentionRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return errors.New("unexpected scan destination count")
	}
	for index, value := range r.values {
		pointer, ok := dest[index].(*int)
		if !ok {
			return errors.New("unexpected scan destination type")
		}
		*pointer = value
	}
	return nil
}

var _ partitionExecutor = (*maintenanceExecutorStub)(nil)
var _ retentionExecutor = (*maintenanceExecutorStub)(nil)

type maintenanceExecutorStub struct {
	retentionExecutorStub
	execCalls int
}

func (s *maintenanceExecutorStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	s.execCalls++
	return pgconn.NewCommandTag("SELECT 1"), nil
}
