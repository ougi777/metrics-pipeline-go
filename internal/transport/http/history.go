package httptransport

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/ougi777/metrics-pipeline-go/internal/service/history"
)

type HistoryHandler struct{ service history.Service }

func NewHistoryHandler(service history.Service) HistoryHandler {
	return HistoryHandler{service: service}
}

func (h HistoryHandler) QueryMetrics(c *gin.Context) {
	query, err := parseHistoryQuery(c)
	if err != nil {
		WriteError(c, http.StatusBadRequest, ErrorCodeInvalidParams, err.Error())
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
	series := make(map[string][]history.Point)
	for _, point := range result.Points {
		series[point.Key] = append(series[point.Key], point)
	}
	// Use a stable JSON shape and leave absent filtered keys out of series.
	responseSeries := make(map[string][]historyResponsePoint, len(series))
	for key, points := range series {
		converted := make([]historyResponsePoint, len(points))
		for i, point := range points {
			converted[i] = historyResponsePoint{Step: point.Step, TS: point.TS, V: point.V, Min: point.Min, Max: point.Max}
		}
		responseSeries[key] = converted
	}
	c.JSON(http.StatusOK, historyResponse{TaskID: query.TaskID, Downsampled: result.Downsampled, BucketMS: result.BucketMS, Series: responseSeries})
}

type historyResponse struct {
	TaskID      string                            `json:"task_id"`
	Downsampled bool                              `json:"downsampled"`
	BucketMS    int64                             `json:"bucket_ms"`
	Series      map[string][]historyResponsePoint `json:"series"`
}
type historyResponsePoint struct {
	Step int32   `json:"step"`
	TS   int64   `json:"ts"`
	V    float64 `json:"v"`
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
}

func parseHistoryQuery(c *gin.Context) (history.Query, error) {
	taskID := c.Param("task_id")
	if taskID == "" || !taskIDPattern.MatchString(taskID) || len(taskID) > maxTaskIDLength {
		return history.Query{}, fmt.Errorf("invalid task_id")
	}
	query := history.Query{TaskID: taskID, MaxPoints: 500}
	if value := c.Query("keys"); value != "" {
		for _, key := range strings.Split(value, ",") {
			key = strings.TrimSpace(key)
			if key == "" || len(key) > maxMetricKeyLength || !metricKeyPattern.MatchString(key) {
				return history.Query{}, fmt.Errorf("invalid keys")
			}
			found := false
			for _, existing := range query.Keys {
				if existing == key {
					found = true
					break
				}
			}
			if !found {
				query.Keys = append(query.Keys, key)
			}
		}
	} else if _, exists := c.GetQuery("keys"); exists {
		return history.Query{}, fmt.Errorf("keys must contain at least one key")
	}
	var err error
	query.From, err = queryInt64(c, "from")
	if err != nil {
		return history.Query{}, err
	}
	query.To, err = queryInt64(c, "to")
	if err != nil {
		return history.Query{}, err
	}
	query.StepFrom, err = queryStep(c, "step_from")
	if err != nil {
		return history.Query{}, err
	}
	query.StepTo, err = queryStep(c, "step_to")
	if err != nil {
		return history.Query{}, err
	}
	if query.From != nil && !validHistoryTimestamp(*query.From) || query.To != nil && !validHistoryTimestamp(*query.To) {
		return history.Query{}, fmt.Errorf("from and to must be positive")
	}
	if query.From != nil && query.To != nil && *query.From >= *query.To {
		return history.Query{}, fmt.Errorf("from must be less than to")
	}
	if query.StepFrom != nil && query.StepTo != nil && *query.StepFrom > *query.StepTo {
		return history.Query{}, fmt.Errorf("step_from must be less than or equal to step_to")
	}
	if value := c.Query("max_points"); value != "" {
		parsed, parseErr := strconv.Atoi(value)
		if parseErr != nil || parsed < 1 || parsed > 5000 {
			return history.Query{}, fmt.Errorf("max_points must be between 1 and 5000")
		}
		query.MaxPoints = parsed
	}
	return query, nil
}

func validHistoryTimestamp(value int64) bool {
	return value > 0 && value <= maxTimestampMillis
}

func queryInt64(c *gin.Context, name string) (*int64, error) {
	value := c.Query(name)
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s must be an integer", name)
	}
	return &parsed, nil
}
func queryStep(c *gin.Context, name string) (*int64, error) {
	value, err := queryInt64(c, name)
	if err != nil {
		return nil, err
	}
	if value != nil && (*value < 0 || *value > maxStepValue) {
		return nil, fmt.Errorf("%s is outside supported range", name)
	}
	return value, nil
}
