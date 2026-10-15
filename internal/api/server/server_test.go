package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/rlsvr/kozlony/internal/api/server"
)

type dummyHandler struct{}

func (d *dummyHandler) GetHealthz(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, server.HealthResponse{Status: "ok"})
}

func (d *dummyHandler) GetReadyz(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, server.HealthResponse{Status: "ready"})
}

func (d *dummyHandler) ListInteractions(ctx echo.Context, params server.ListInteractionsParams) error {
	return ctx.NoContent(http.StatusOK)
}

func (d *dummyHandler) CreateInteraction(ctx echo.Context, params server.CreateInteractionParams) error {
	return ctx.NoContent(http.StatusCreated)
}

func (d *dummyHandler) EventsInteractions(ctx echo.Context, params server.EventsInteractionsParams) error {
	return ctx.NoContent(http.StatusOK)
}

func (d *dummyHandler) GetInteraction(ctx echo.Context, id openapi_types.UUID) error {
	return ctx.NoContent(http.StatusOK)
}

func (d *dummyHandler) UpdateInteraction(ctx echo.Context, id openapi_types.UUID) error {
	return ctx.NoContent(http.StatusOK)
}

func TestServer_InitializationAndPprofEndpoints(t *testing.T) {
	h := &dummyHandler{}
	e := server.New(h)

	if e == nil {
		t.Fatal("expected non-nil echo server")
	}

	// Verify standard API routes
	req := httptest.NewRequest(http.MethodGet, "/v1/healthz", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 from /v1/healthz, got %d", rec.Code)
	}

	// Verify pprof diagnostic endpoints
	pprofEndpoints := []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/symbol",
	}

	for _, ep := range pprofEndpoints {
		t.Run("pprof_"+ep, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, ep, nil)
			w := httptest.NewRecorder()
			e.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Errorf("expected 200 from %s, got %d", ep, w.Code)
			}
		})
	}

	// Trigger 404 to exercise the error branch in RequestLogger
	errReq := httptest.NewRequest(http.MethodGet, "/not-found-route-triggering-error", nil)
	errRec := httptest.NewRecorder()
	e.ServeHTTP(errRec, errReq)
	if errRec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", errRec.Code)
	}
}
