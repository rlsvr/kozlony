package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"

	"kozlony/internal/api/handler"
	"kozlony/internal/api/server"
)

func TestHealthEndpoints(t *testing.T) {
	e := echo.New()
	h := handler.New()

	// Test Healthz
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/healthz", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := h.GetHealthz(c); err != nil {
		t.Fatalf("unexpected error from GetHealthz: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp server.HealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("expected status ok, got %s", resp.Status)
	}

	// Test Readyz
	reqReady := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/readyz", nil)
	recReady := httptest.NewRecorder()
	cReady := e.NewContext(reqReady, recReady)

	if err := h.GetReadyz(cReady); err != nil {
		t.Fatalf("unexpected error from GetReadyz: %v", err)
	}
	if recReady.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", recReady.Code)
	}
}
