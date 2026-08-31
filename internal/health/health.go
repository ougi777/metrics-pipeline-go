package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

type Check func(context.Context) error

type State struct {
	ready atomic.Bool
}

func NewState() *State           { return &State{} }
func (s *State) SetReady(v bool) { s.ready.Store(v) }
func (s *State) Ready() bool     { return s.ready.Load() }

type Handler struct {
	State        *State
	Postgres     Check
	RabbitMQ     Check
	ProbeTimeout time.Duration
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/healthz" {
		write(w, http.StatusOK, map[string]any{"status": "ok"})
		return
	}
	if r.URL.Path != "/readyz" {
		http.NotFound(w, r)
		return
	}
	if h.State == nil || !h.State.Ready() {
		write(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.ProbeTimeout)
	if h.ProbeTimeout <= 0 {
		ctx, cancel = context.WithTimeout(r.Context(), 2*time.Second)
	}
	defer cancel()
	checks := map[string]string{}
	status := http.StatusOK
	for name, check := range map[string]Check{"postgres": h.Postgres, "rabbitmq": h.RabbitMQ} {
		if check == nil {
			checks[name] = "unconfigured"
			status = http.StatusServiceUnavailable
			continue
		}
		if err := check(ctx); err != nil {
			checks[name] = "failed"
			status = http.StatusServiceUnavailable
		} else {
			checks[name] = "ok"
		}
	}
	result := map[string]any{"status": "ready", "checks": checks}
	if status != http.StatusOK {
		result["status"] = "not_ready"
	}
	write(w, status, result)
}

func write(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
