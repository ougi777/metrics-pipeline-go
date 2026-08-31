package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHandler(t *testing.T) {
	s := NewState()
	h := Handler{State: s, ProbeTimeout: time.Second, Postgres: func(_ context.Context) error { return nil }, RabbitMQ: func(_ context.Context) error { return nil }}
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if r.Code != http.StatusOK {
		t.Fatalf("healthz status = %d", r.Code)
	}
	r = httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if r.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready before state status = %d", r.Code)
	}
	s.SetReady(true)
	r = httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if r.Code != http.StatusOK {
		t.Fatalf("ready status = %d", r.Code)
	}
	h.Postgres = func(_ context.Context) error { return errors.New("down") }
	r = httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if r.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed dependency status = %d", r.Code)
	}
}
