package httptransport

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ougi777/metrics-pipeline-go/internal/service/history"
)

type historyRepositoryFunc func(context.Context, history.Query) (history.Result, error)

func (f historyRepositoryFunc) QueryHistory(ctx context.Context, query history.Query) (history.Result, error) {
	return f(ctx, query)
}

func TestQueryMetricsReturnsFilteredResponse(t *testing.T) {
	var got history.Query
	repository := historyRepositoryFunc(func(_ context.Context, query history.Query) (history.Result, error) {
		got = query
		return history.Result{Exists: true, Points: []history.Point{{Key: "loss", Step: 3, TS: 1000, V: 1.5, Min: 1.5, Max: 1.5}}}, nil
	})
	router := NewRouter(RouterOptions{HistoryService: history.NewService(repository)})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task-a/metrics?keys=loss%2Closs&from=1000&to=2000&step_from=2&step_to=4&max_points=10", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if got.TaskID != "task-a" || len(got.Keys) != 1 || got.Keys[0] != "loss" || got.MaxPoints != 10 {
		t.Fatalf("query = %#v", got)
	}
	var body struct {
		Downsampled bool                              `json:"downsampled"`
		BucketMS    int64                             `json:"bucket_ms"`
		Series      map[string][]historyResponsePoint `json:"series"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	point := body.Series["loss"][0]
	if point.Step != 3 || point.TS != 1000 || point.V != 1.5 || point.Min != 1.5 || point.Max != 1.5 {
		t.Fatalf("point = %#v", point)
	}
	if body.Downsampled || body.BucketMS != 0 {
		t.Fatalf("sampling metadata = %v/%d", body.Downsampled, body.BucketMS)
	}
}

func TestQueryMetricsDistinguishesMissingTaskAndEmptyFilter(t *testing.T) {
	tests := map[string]struct {
		exists     bool
		wantStatus int
		wantSeries bool
	}{
		"missing":      {exists: false, wantStatus: http.StatusNotFound},
		"empty filter": {exists: true, wantStatus: http.StatusOK, wantSeries: true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			router := NewRouter(RouterOptions{HistoryService: history.NewService(historyRepositoryFunc(func(context.Context, history.Query) (history.Result, error) {
				return history.Result{Exists: test.exists}, nil
			}))})
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task-a/metrics", nil))
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
			if test.wantSeries && response.Body.String() == "" {
				t.Fatal("empty response")
			}
		})
	}
}

func TestQueryMetricsRejectsInvalidParameters(t *testing.T) {
	router := NewRouter(RouterOptions{HistoryService: history.NewService(historyRepositoryFunc(func(context.Context, history.Query) (history.Result, error) { return history.Result{Exists: true}, nil }))})
	for _, path := range []string{
		"/api/v1/tasks/task-a/metrics?from=2&to=1",
		"/api/v1/tasks/task-a/metrics?step_from=3&step_to=2",
		"/api/v1/tasks/task-a/metrics?max_points=5001",
		"/api/v1/tasks/task-a/metrics?keys=loss,,lr",
		"/api/v1/tasks/task-a/metrics?keys=",
		"/api/v1/tasks/task-a/metrics?from=253402300800000",
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusBadRequest {
			t.Errorf("%s status = %d, want 400", path, response.Code)
		}
	}
}

func TestQueryMetricsReturnsInternalError(t *testing.T) {
	router := NewRouter(RouterOptions{HistoryService: history.NewService(historyRepositoryFunc(func(context.Context, history.Query) (history.Result, error) {
		return history.Result{}, errors.New("database down")
	}))})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task-a/metrics", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
}
