package postgres

import (
	"testing"
	"time"

	"github.com/ougi777/metrics-pipeline-go/internal/domain"
)

func TestNewMetricPointStoreRejectsNilPool(t *testing.T) {
	if _, err := NewMetricPointStore(nil); err == nil {
		t.Fatal("NewMetricPointStore() error = nil, want missing pool error")
	}
}

func TestEncodeMetricPointsBuildsColumnsAndSortsTasks(t *testing.T) {
	points := []domain.MetricPoint{
		{TaskID: "task-b", Key: "loss", Step: 2, TimestampMillis: 1756089600123, Value: 1.2},
		{TaskID: "task-a", Key: "lr", Step: 3, TimestampMillis: 1756089601123, Value: 0.001},
		{TaskID: "task-b", Key: "lr", Step: 2, TimestampMillis: 1756089600123, Value: 0.002},
	}

	columns, taskIDs, err := encodeMetricPoints(points)
	if err != nil {
		t.Fatalf("encodeMetricPoints() error = %v", err)
	}
	if len(columns.taskIDs) != len(points) || len(columns.keys) != len(points) || len(columns.steps) != len(points) || len(columns.timestamps) != len(points) || len(columns.values) != len(points) {
		t.Fatalf("encoded column lengths do not match point count: %#v", columns)
	}
	if len(taskIDs) != 2 || taskIDs[0] != "task-a" || taskIDs[1] != "task-b" {
		t.Fatalf("unique task IDs = %v, want [task-a task-b]", taskIDs)
	}
	if got := columns.timestamps[0]; !got.Equal(time.UnixMilli(points[0].TimestampMillis).UTC()) {
		t.Fatalf("timestamp = %s, want unix milliseconds conversion", got)
	}
}

func TestEncodeMetricPointsRejectsStepOutsideDatabaseRange(t *testing.T) {
	points := []domain.MetricPoint{{TaskID: "task-a", Key: "loss", Step: int64(^uint32(0)>>1) + 1, TimestampMillis: 1, Value: 1}}

	if _, _, err := encodeMetricPoints(points); err == nil {
		t.Fatal("encodeMetricPoints() error = nil, want int32 range error")
	}
}
