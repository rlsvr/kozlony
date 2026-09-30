// Package handler provides the HTTP handler implementation of the OpenAPI ServerInterface.
package handler

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/nats-io/nats.go/jetstream"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"github.com/rs/zerolog/log"

	"kozlony/internal/api/server"
	"kozlony/internal/database"
	"kozlony/internal/messaging/events"
)

var _ server.ServerInterface = (*Handler)(nil)

// Publisher sends interaction events to the JetStream message broker.
type Publisher interface {
	PublishInteractionCreated(ctx context.Context, evt *events.InteractionCreatedEvent) (*jetstream.PubAck, error)
}

// Handler implements server.ServerInterface.
type Handler struct {
	publisher Publisher
	repo      database.Repository
}

// New creates a new Handler instance with publisher and database repository dependencies.
func New(pub Publisher, repo database.Repository) *Handler {
	return &Handler{
		publisher: pub,
		repo:      repo,
	}
}

// GetHealthz handles liveness probe requests.
func (*Handler) GetHealthz(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, server.HealthResponse{Status: "ok"})
}

// GetReadyz handles readiness probe requests.
func (*Handler) GetReadyz(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, server.HealthResponse{Status: "ok"})
}

// CreateInteraction handles creating new posts and replies, publishing them to JetStream.
func (h *Handler) CreateInteraction(ctx echo.Context) error {
	var req server.CreateInteractionRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{Error: "invalid request payload"})
	}

	if req.Author == "" || req.Body == "" {
		return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{Error: "author and body are required"})
	}

	groupID := "root"
	if req.GroupId != nil && *req.GroupId != "" {
		groupID = *req.GroupId
	}

	newID, err := uuid.NewV7()
	if err != nil {
		log.Error().Err(err).Msg("failed to generate UUIDv7")
		return ctx.JSON(http.StatusInternalServerError, server.ErrorResponse{Error: "failed to generate interaction ID"})
	}

	now := time.Now().UTC()
	depth := 0
	var parentIDStr *string
	var rootIDStr *string

	if req.ParentId != nil {
		pIDStr := req.ParentId.String()
		parentIDStr = &pIDStr
		depth = 1
		rootIDStr = &pIDStr
	}

	evt := events.InteractionCreatedEvent{
		Author:     req.Author,
		Body:       req.Body,
		CreatedAt:  now,
		Depth:      depth,
		GroupID:    groupID,
		ID:         newID.String(),
		ParentID:   parentIDStr,
		ReplyCount: 0,
		RootID:     rootIDStr,
		Title:      req.Title,
	}

	if h.publisher != nil {
		if _, err := h.publisher.PublishInteractionCreated(ctx.Request().Context(), &evt); err != nil {
			log.Error().Err(err).Str("id", newID.String()).Msg("failed to publish interaction created event")
			return ctx.JSON(http.StatusInternalServerError, server.ErrorResponse{Error: "failed to persist interaction"})
		}
	}

	resp := server.Interaction{
		Id:         newID,
		GroupId:    groupID,
		Author:     req.Author,
		Body:       req.Body,
		Title:      req.Title,
		CreatedAt:  now,
		ReplyCount: 0,
		Depth:      depth,
		ParentId:   req.ParentId,
	}

	return ctx.JSON(http.StatusCreated, resp)
}

// ListInteractions handles listing root posts for the main feed.
func (h *Handler) ListInteractions(ctx echo.Context, params server.ListInteractionsParams) error {
	if h.repo == nil {
		return ctx.JSON(http.StatusOK, server.InteractionFeedResponse{
			Interactions: []server.Interaction{},
		})
	}

	groupID := "root"
	if params.GroupId != nil && *params.GroupId != "" {
		groupID = *params.GroupId
	}

	limit := 20
	if params.Limit != nil && *params.Limit > 0 {
		limit = *params.Limit
	}

	items, err := h.repo.ListFeed(ctx.Request().Context(), groupID, limit, params.Cursor)
	if err != nil {
		log.Error().Err(err).Msg("failed to list feed from database")
		return ctx.JSON(http.StatusInternalServerError, server.ErrorResponse{Error: "failed to load interactions"})
	}

	interactions := make([]server.Interaction, len(items))
	for i, item := range items {
		interactions[i] = toAPIInteraction(item)
	}

	var nextCursor *time.Time
	if len(items) == limit {
		nextCursor = &items[len(items)-1].CreatedAt
	}

	return ctx.JSON(http.StatusOK, server.InteractionFeedResponse{
		Interactions: interactions,
		NextCursor:   nextCursor,
	})
}

const errInteractionNotFound = "interaction not found"

// GetInteraction handles fetching a post along with its replies.
func (h *Handler) GetInteraction(ctx echo.Context, id openapi_types.UUID) error {
	if h.repo == nil {
		return ctx.JSON(http.StatusNotFound, server.ErrorResponse{Error: errInteractionNotFound})
	}

	thread, err := h.repo.GetThread(ctx.Request().Context(), id)
	if err != nil {
		if errors.Is(err, database.ErrNotFound) {
			return ctx.JSON(http.StatusNotFound, server.ErrorResponse{Error: errInteractionNotFound})
		}
		log.Error().Err(err).Str("id", id.String()).Msg("failed to get thread from database")
		return ctx.JSON(http.StatusInternalServerError, server.ErrorResponse{Error: "failed to load thread"})
	}

	if len(thread) == 0 {
		return ctx.JSON(http.StatusNotFound, server.ErrorResponse{Error: errInteractionNotFound})
	}

	root := toAPIInteraction(thread[0])
	replies := make([]server.Interaction, 0, len(thread)-1)
	for _, item := range thread[1:] {
		replies = append(replies, toAPIInteraction(item))
	}

	return ctx.JSON(http.StatusOK, server.InteractionDetail{
		Interaction: root,
		Replies:     replies,
	})
}

// SubscribeLiveEvents handles real-time SSE streaming.
func (*Handler) SubscribeLiveEvents(ctx echo.Context, _ server.SubscribeLiveEventsParams) error {
	return ctx.NoContent(http.StatusNotImplemented)
}

func toAPIInteraction(item *database.Interaction) server.Interaction {
	res := server.Interaction{
		Id:         item.ID,
		GroupId:    item.GroupID,
		Author:     item.Author,
		Body:       item.Body,
		Title:      item.Title,
		CreatedAt:  item.CreatedAt,
		UpdatedAt:  item.UpdatedAt,
		ReplyCount: item.ReplyCount,
		Depth:      item.Depth,
	}
	if item.RootID != nil {
		res.RootId = item.RootID
	}
	if item.ParentID != nil {
		res.ParentId = item.ParentID
	}
	return res
}
