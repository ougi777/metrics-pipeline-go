package httptransport

import (
	"net/http"

	"github.com/gin-gonic/gin"
	ingestservice "github.com/ougi777/metrics-pipeline-go/internal/service/ingest"
)

type RouterOptions struct {
	IngestService ingestservice.Service
}

func NewRouter(options RouterOptions) http.Handler {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(gin.Recovery())

	handler := NewIngestHandler(options.IngestService)
	api := router.Group("/api/v1")
	api.POST("/ingest/metrics", handler.IngestMetrics)

	return router
}
