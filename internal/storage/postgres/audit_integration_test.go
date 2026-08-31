//go:build integration

package postgres

import (
	"context"
	"github.com/ougi777/metrics-pipeline-go/internal/service/audit"
	"testing"
	"time"
)

func TestMetricPointStoreAuditCountsDuplicatesAndGaps(t *testing.T) {
	pool, store := newMetricStoreIntegrationDatabase(t)
	ctx := context.Background()
	taskID := "audit-integration-task"
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "DELETE FROM metric_points WHERE task_id = $1", taskID) })
	now := time.Now().UTC()
	for _, row := range []struct {
		key  string
		step int
		ts   time.Time
	}{
		{"loss", 1, now}, {"loss", 1, now.Add(time.Second)}, {"lr", 3, now},
	} {
		if _, err := pool.Exec(ctx, `INSERT INTO metric_points (task_id,key,step,ts,value) VALUES ($1,$2,$3,$4,1)`, taskID, row.key, row.step, row.ts); err != nil {
			t.Fatal(err)
		}
	}
	result, err := store.QueryAudit(ctx, audit.Query{TaskID: taskID})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Exists || result.PointCount != 3 || result.DistinctSteps != 2 || *result.FirstStep != 1 || *result.LastStep != 3 {
		t.Fatalf("unexpected audit result: %+v", result)
	}
	if len(result.MissingSteps) != 1 || result.MissingSteps[0] != 2 {
		t.Fatalf("missing steps = %v", result.MissingSteps)
	}
}
