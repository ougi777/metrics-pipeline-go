package httptransport

import (
	"github.com/gin-gonic/gin"
	"github.com/ougi777/metrics-pipeline-go/internal/service/audit"
	"net/http"
)

type AuditHandler struct{ service audit.Service }

func NewAuditHandler(service audit.Service) AuditHandler { return AuditHandler{service: service} }
func (h AuditHandler) GetAudit(c *gin.Context) {
	query := audit.Query{TaskID: c.Param("task_id")}
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
	c.JSON(http.StatusOK, auditResponse{TaskID: query.TaskID, PointCount: result.PointCount, DistinctSteps: result.DistinctSteps, FirstStep: result.FirstStep, LastStep: result.LastStep, Keys: result.Keys, MissingSteps: result.MissingSteps})
}

type auditResponse struct {
	TaskID        string   `json:"task_id"`
	PointCount    int64    `json:"point_count"`
	DistinctSteps int64    `json:"distinct_steps"`
	FirstStep     *int32   `json:"first_step"`
	LastStep      *int32   `json:"last_step"`
	Keys          []string `json:"keys"`
	MissingSteps  []int32  `json:"missing_steps"`
}
