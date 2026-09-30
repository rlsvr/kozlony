package handler

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -source=handler.go -destination=mocks/mock_handler.go -package=mocks

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

const (
	errInteractionNotFound = "interaction not found"
	maxTitleLength         = 255
	maxBodyLength          = 65535
	maxAuthorLength        = 100
)

// Publisher sends interaction events to the JetStream message broker.
type Publisher interface {
	PublishInteractionCreated(ctx context.Context, evt *events.InteractionCreatedEvent) (*jetstream.PubAck, error)
	PublishInteractionEdited(ctx context.Context, evt *events.InteractionEditedEvent) (*jetstream.PubAck, error)
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
func (h *Handler) CreateInteraction(ctx echo.Context, params server.CreateInteractionParams) error {
	var req server.CreateInteractionRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{Error: "invalid request payload"})
	}

	if req.Author == "" || len(req.Author) > maxAuthorLength {
		return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{Error: "author is required and must not exceed 100 characters"})
	}
	if req.Body == "" || len(req.Body) > maxBodyLength {
		return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{Error: "body is required and must not exceed 65535 characters"})
	}
	if req.Title != nil && len(*req.Title) > maxTitleLength {
		return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{Error: "title must not exceed 255 characters"})
	}

	groupID := "root"
	if req.GroupId != nil && *req.GroupId != "" {
		groupID = *req.GroupId
	}

	idempotencyKey := ""
	if params.IdempotencyKey != nil && *params.IdempotencyKey != "" {
		idempotencyKey = *params.IdempotencyKey
	} else if hKey := ctx.Request().Header.Get("Idempotency-Key"); hKey != "" {
		idempotencyKey = hKey
	}

	newID, err := resolveInteractionID(idempotencyKey)
	if err != nil {
		log.Error().Err(err).Msg("failed to generate UUIDv7")
		return ctx.JSON(http.StatusInternalServerError, server.ErrorResponse{Error: "failed to generate interaction ID"})
	}

	if idempotencyKey != "" && h.repo != nil {
		existing, err := h.repo.GetByID(ctx.Request().Context(), newID)
		if err == nil && existing != nil {
			return ctx.JSON(http.StatusOK, toAPIInteraction(existing))
		}
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
		Version:    1,
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
		Version:    1,
	}

	return ctx.JSON(http.StatusCreated, resp)
}

// UpdateInteraction handles editing an interaction's title and body with version verification.
func (h *Handler) UpdateInteraction(ctx echo.Context, id openapi_types.UUID) error {
	var req server.UpdateInteractionRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{Error: "invalid request payload"})
	}

	if req.Body == "" || len(req.Body) > maxBodyLength {
		return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{Error: "body is required and must not exceed 65535 characters"})
	}
	if req.Title != nil && len(*req.Title) > maxTitleLength {
		return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{Error: "title must not exceed 255 characters"})
	}
	if req.Version < 1 {
		return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{Error: "version must be a positive integer"})
	}

	if h.repo == nil {
		return ctx.JSON(http.StatusNotFound, server.ErrorResponse{Error: errInteractionNotFound})
	}

	updated, err := h.repo.Update(ctx.Request().Context(), id, req.Version, req.Title, req.Body)
	if err != nil {
		if errors.Is(err, database.ErrNotFound) {
			return ctx.JSON(http.StatusNotFound, server.ErrorResponse{Error: errInteractionNotFound})
		}
		if errors.Is(err, database.ErrVersionConflict) {
			return ctx.JSON(http.StatusConflict, server.ErrorResponse{Error: "version conflict: interaction was modified by another client"})
		}
		log.Error().Err(err).Str("id", id.String()).Msg("failed to update interaction")
		return ctx.JSON(http.StatusInternalServerError, server.ErrorResponse{Error: "failed to update interaction"})
	}

	if h.publisher != nil {
		updatedAt := time.Now().UTC()
		if updated.UpdatedAt != nil {
			updatedAt = *updated.UpdatedAt
		}
		evt := events.InteractionEditedEvent{
			Body:      updated.Body,
			GroupID:   updated.GroupID,
			ID:        updated.ID.String(),
			Title:     updated.Title,
			UpdatedAt: updatedAt,
			Version:   updated.Version,
		}
		if _, err := h.publisher.PublishInteractionEdited(ctx.Request().Context(), &evt); err != nil {
			log.Error().Err(err).Str("id", id.String()).Int("version", updated.Version).Msg("failed to publish interaction edited event")
		}
	}

	return ctx.JSON(http.StatusOK, toAPIInteraction(updated))
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
		Version:    item.Version,
	}
	if item.RootID != nil {
		res.RootId = item.RootID
	}
	if item.ParentID != nil {
		res.ParentId = item.ParentID
	}
	return res
}

func resolveInteractionID(idempotencyKey string) (uuid.UUID, error) {
	if idempotencyKey == "" {
		return uuid.NewV7()
	}
	if parsed, err := uuid.Parse(idempotencyKey); err == nil {
		return parsed, nil
	}
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(idempotencyKey)), nil
}
