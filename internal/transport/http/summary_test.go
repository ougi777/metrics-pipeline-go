package httptransport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ougi777/metrics-pipeline-go/internal/service/summary"
)

type summaryRepositoryFunc func(context.Context, summary.Query) (summary.Result, error)

func (f summaryRepositoryFunc) QuerySummary(ctx context.Context, query summary.Query) (summary.Result, error) {
	return f(ctx, query)
}

func TestGetSummaryReturnsAggregatedResponse(t *testing.T) {
	router := NewRouter(RouterOptions{SummaryService: summary.NewService(summaryRepositoryFunc(func(_ context.Context, query summary.Query) (summary.Result, error) {
		if query.TaskID != "task-a" {
			t.Fatalf("task id = %q, want task-a", query.TaskID)
		}
		return summary.Result{
			Exists:    true,
			LastStep:  7,
			UpdatedAt: 123456,
			Metrics:   map[string]summary.Metric{"loss": {Last: 1.5, Min: 1, Max: 2, Avg: 1.25}},
		}, nil
	}))})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task-a/summary", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if !contains(response.Body.String(), `"last_step":7`) || !contains(response.Body.String(), `"updated_at":123456`) || !contains(response.Body.String(), `"avg":1.25`) {
		t.Fatalf("body = %s", response.Body.String())
	}
}

func TestGetSummaryReturnsNotFoundAndInternalError(t *testing.T) {
	tests := []struct {
		name       string
		result     summary.Result
		err        error
		wantStatus int
	}{
		{name: "missing", wantStatus: http.StatusNotFound},
		{name: "internal", err: errors.New("database down"), wantStatus: http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			router := NewRouter(RouterOptions{SummaryService: summary.NewService(summaryRepositoryFunc(func(context.Context, summary.Query) (summary.Result, error) {
				return test.result, test.err
			}))})
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task-a/summary", nil))
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
		})
	}
}

func TestGetSummaryRejectsInvalidTaskID(t *testing.T) {
	router := NewRouter(RouterOptions{SummaryService: summary.NewService(summaryRepositoryFunc(func(context.Context, summary.Query) (summary.Result, error) {
		t.Fatal("repository called for invalid task id")
		return summary.Result{}, nil
	}))})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/-bad/summary", nil))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
}

func contains(value, fragment string) bool {
	for i := 0; i+len(fragment) <= len(value); i++ {
		if value[i:i+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
