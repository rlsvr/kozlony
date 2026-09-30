// Package handler provides the HTTP handler implementation of the OpenAPI ServerInterface.
package handler

import (
	"net/http"

	"github.com/labstack/echo/v4"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"kozlony/internal/api/server"
)

var _ server.ServerInterface = (*Handler)(nil)

// Handler implements server.ServerInterface.
type Handler struct{}

// New creates a new Handler instance.
func New() *Handler {
	return &Handler{}
}

// GetHealthz handles liveness probe requests.
func (*Handler) GetHealthz(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, server.HealthResponse{Status: "ok"})
}

// GetReadyz handles readiness probe requests.
func (*Handler) GetReadyz(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, server.HealthResponse{Status: "ok"})
}

// CreateInteraction handles creating new posts and replies.
func (*Handler) CreateInteraction(ctx echo.Context) error {
	return ctx.NoContent(http.StatusNotImplemented)
}

// ListInteractions handles listing root posts for the main feed.
func (*Handler) ListInteractions(ctx echo.Context, _ server.ListInteractionsParams) error {
	return ctx.NoContent(http.StatusNotImplemented)
}

// GetInteraction handles fetching a post along with its replies.
func (*Handler) GetInteraction(ctx echo.Context, _ openapi_types.UUID) error {
	return ctx.NoContent(http.StatusNotImplemented)
}

// SubscribeLiveEvents handles real-time SSE streaming.
func (*Handler) SubscribeLiveEvents(ctx echo.Context, _ server.SubscribeLiveEventsParams) error {
	return ctx.NoContent(http.StatusNotImplemented)
}
