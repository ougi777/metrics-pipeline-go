package httptransport

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ougi777/metrics-pipeline-go/internal/service/events"
	"github.com/ougi777/metrics-pipeline-go/internal/sse"
)

type StreamHandler struct {
	service           events.Service
	hub               *sse.Hub
	heartbeatInterval time.Duration
}

func NewStreamHandler(service events.Service, hub *sse.Hub) StreamHandler {
	return StreamHandler{service: service, hub: hub, heartbeatInterval: 15 * time.Second}
}

func (h StreamHandler) StreamMetrics(c *gin.Context) {
	taskID := c.Param("task_id")
	if taskID == "" || !taskIDPattern.MatchString(taskID) || len(taskID) > maxTaskIDLength {
		WriteError(c, http.StatusBadRequest, ErrorCodeInvalidParams, "invalid task_id")
		return
	}

	//after是sse客户端上一次接收到的seq
	after, hasCursor, err := parseLastEventID(c.GetHeader("Last-Event-ID"), taskID)
	if err != nil {
		WriteError(c, http.StatusBadRequest, ErrorCodeInvalidParams, err.Error())
		return
	}

	//rwlock锁维护map建立sse长连接
	sub := h.hub.Subscribe(taskID)
	defer sub.Close()

	//查询该task窗口内的最小seq 与最大seq(最新)
	oldest, latest, err := h.service.Bounds(c.Request.Context(), taskID)
	if err != nil {
		WriteError(c, http.StatusInternalServerError, ErrorCodeInternal, "internal server error")
		return
	}

	if hasCursor && ((oldest > 0 && after < oldest-1) || (oldest == 0 && latest > after)) {
		WriteError(c, http.StatusBadRequest, ErrorCodeInvalidParams, "Last-Event-ID is outside retention window")
		return
	}

	//默认最新开始
	if !hasCursor {
		after = latest
	}

	//查询after 到 最新seq之间的数据补发
	eventsToSend, err := h.service.Query(c.Request.Context(), taskID, after)
	if err != nil {
		WriteError(c, http.StatusInternalServerError, ErrorCodeInternal, "internal server error")
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Status(http.StatusOK)
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		return
	}
	w := bufio.NewWriter(c.Writer)
	writeEvent := func(event sse.Event) error {
		//小于当前sse客户端接收到的最新seq，拒收
		if event.EventSeq <= after {
			return nil
		}
		payload := strings.TrimSpace(string(event.Payload))
		if _, err := fmt.Fprintf(w, "event: metrics\nid: %s:%d\ndata: %s\n\n", event.TaskID, event.EventSeq, payload); err != nil {
			return err
		}
		if err := w.Flush(); err != nil {
			return err
		}
		flusher.Flush()
		//更新最新消息seq
		after = event.EventSeq
		return nil
	}
	for _, event := range eventsToSend {
		if err := writeEvent(event); err != nil {
			return
		}
	}
	ticker := time.NewTicker(h.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case event, ok := <-sub.C:
			if !ok {
				return
			}
			if err := writeEvent(event); err != nil {
				return
			}
		case now := <-ticker.C: //15s探活
			if _, err := fmt.Fprintf(w, "event: ping\ndata: %s\n\n", mustJSON(map[string]int64{"ts": now.UnixMilli()})); err != nil {
				return
			}
			if err := w.Flush(); err != nil {
				return
			}
			flusher.Flush()
		case <-c.Request.Context().Done():
			return
		}
	}
}

func parseLastEventID(value, taskID string) (int64, bool, error) {
	if value == "" {
		return 0, false, nil
	}
	parts := strings.Split(value, ":")
	if len(parts) != 2 || parts[0] != taskID {
		return 0, false, fmt.Errorf("invalid Last-Event-ID")
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || seq < 0 {
		return 0, false, fmt.Errorf("invalid Last-Event-ID")
	}
	return seq, true, nil
}

func mustJSON(value any) string { data, _ := json.Marshal(value); return string(data) }
