package httptransport

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ougi777/metrics-pipeline-go/internal/service/events"
	"github.com/ougi777/metrics-pipeline-go/internal/service/history"
	ingestservice "github.com/ougi777/metrics-pipeline-go/internal/service/ingest"
	"github.com/ougi777/metrics-pipeline-go/internal/service/summary"
	"github.com/ougi777/metrics-pipeline-go/internal/sse"
)

type RouterOptions struct {
	IngestService  ingestservice.Service
	HistoryService history.Service
	SummaryService summary.Service
	EventsService  events.Service
	EventHub       *sse.Hub
}

func NewRouter(options RouterOptions) http.Handler {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(gin.Recovery())
	if options.EventHub == nil {
		options.EventHub = sse.NewHub()
	}

	handler := NewIngestHandler(options.IngestService)
	historyHandler := NewHistoryHandler(options.HistoryService)
	summaryHandler := NewSummaryHandler(options.SummaryService)
	streamHandler := NewStreamHandler(options.EventsService, options.EventHub)
	api := router.Group("/api/v1")
	api.POST("/ingest/metrics", handler.IngestMetrics)
	api.GET("/tasks/:task_id/metrics", historyHandler.QueryMetrics)
	api.GET("/tasks/:task_id/summary", summaryHandler.GetSummary)
	api.GET("/tasks/:task_id/metrics/stream", streamHandler.StreamMetrics)

	return router
}
