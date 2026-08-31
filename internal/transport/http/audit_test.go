package httptransport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ougi777/metrics-pipeline-go/internal/service/audit"
)

type auditRepositoryFunc func(context.Context, audit.Query) (audit.Result, error)

func (f auditRepositoryFunc) QueryAudit(ctx context.Context, q audit.Query) (audit.Result, error) {
	return f(ctx, q)
}

func TestGetAuditResponseShape(t *testing.T) {
	router := NewRouter(RouterOptions{AuditService: audit.NewService(auditRepositoryFunc(func(_ context.Context, _ audit.Query) (audit.Result, error) {
		return audit.Result{Exists: true, PointCount: 4, DistinctSteps: 3, FirstStep: ptr32(1), LastStep: ptr32(3), Keys: []string{"loss", "lr"}, MissingSteps: []int32{2}}, nil
	}))})
	r := httptest.NewRecorder()
	router.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/api/v1/admin/tasks/task-a/audit", nil))
	if r.Code != http.StatusOK {
		t.Fatalf("status = %d", r.Code)
	}
	body := r.Body.String()
	for _, field := range []string{`"task_id":"task-a"`, `"point_count":4`, `"distinct_steps":3`, `"first_step":1`, `"last_step":3`, `"keys":["loss","lr"]`, `"missing_steps":[2]`} {
		if !contains(body, field) {
			t.Fatalf("body %s missing %s", body, field)
		}
	}
}

func ptr32(v int32) *int32 { return &v }
