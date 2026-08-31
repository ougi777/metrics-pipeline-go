package httptransport

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ougi777/metrics-pipeline-go/internal/service/summary"
)

type SummaryHandler struct{ service summary.Service }

func NewSummaryHandler(service summary.Service) SummaryHandler {
	return SummaryHandler{service: service}
}

func (h SummaryHandler) GetSummary(c *gin.Context) {
	query := summary.Query{TaskID: c.Param("task_id")}
	if query.TaskID == "" || !taskIDPattern.MatchString(query.TaskID) || len(query.TaskID) > maxTaskIDLength {
		WriteError(c, http.StatusBadRequest, ErrorCodeInvalidParams, "invalid task_id")
		return
	}

	result, err := h.service.Query(c.Request.Context(), query)
	if err != nil {
		WriteError(c, http.StatusInternalServerError, ErrorCodeInternal, "internal server error")
		return
	}
	if !result.Exists {
		WriteError(c, http.StatusNotFound, ErrorCodeTaskNotFound, "task not found")
		return
	}

	metrics := make(map[string]summaryResponseMetric, len(result.Metrics))
	for key, metric := range result.Metrics {
		metrics[key] = summaryResponseMetric{Last: metric.Last, Min: metric.Min, Max: metric.Max, Avg: metric.Avg}
	}
	c.JSON(http.StatusOK, summaryResponse{
		TaskID:    query.TaskID,
		LastStep:  result.LastStep,
		UpdatedAt: result.UpdatedAt,
		Metrics:   metrics,
	})
}

type summaryResponse struct {
	TaskID    string                           `json:"task_id"`
	LastStep  int32                            `json:"last_step"`
	UpdatedAt int64                            `json:"updated_at"`
	Metrics   map[string]summaryResponseMetric `json:"metrics"`
}

type summaryResponseMetric struct {
	Last float64 `json:"last"`
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Avg  float64 `json:"avg"`
}
