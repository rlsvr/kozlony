package handler

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -source=handler.go -destination=mocks/mock_handler.go -package=mocks

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
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

// Publisher sends interaction events to the JetStream message broker and subscribes to real-time events.
type Publisher interface {
	Ping(ctx context.Context) error
	PublishInteractionCreated(ctx context.Context, evt *events.InteractionCreatedEvent) (*jetstream.PubAck, error)
	PublishInteractionEdited(ctx context.Context, evt *events.InteractionEditedEvent) (*jetstream.PubAck, error)
	SubscribeEvents(ctx context.Context, groupID *string) (<-chan []byte, func(), error)
}

type cachedInteraction struct {
	ID     uuid.UUID
	RootID *uuid.UUID
	Depth  int
}

// Handler implements server.ServerInterface.
type Handler struct {
	publisher          Publisher
	repo               database.Repository
	recentInteractions sync.Map
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

// GetReadyz handles readiness probe requests by verifying database and messaging health.
func (h *Handler) GetReadyz(ctx echo.Context) error {
	reqCtx := ctx.Request().Context()

	if h.repo == nil || h.publisher == nil {
		return ctx.JSON(http.StatusServiceUnavailable, server.ErrorResponse{
			Error: "service dependencies not initialized",
		})
	}

	if err := h.repo.Ping(reqCtx); err != nil {
		log.Warn().Err(err).Msg("readiness probe: database ping failed")
		return ctx.JSON(http.StatusServiceUnavailable, server.ErrorResponse{
			Error: "database unavailable",
		})
	}

	if err := h.publisher.Ping(reqCtx); err != nil {
		log.Warn().Err(err).Msg("readiness probe: nats ping failed")
		return ctx.JSON(http.StatusServiceUnavailable, server.ErrorResponse{
			Error: "messaging broker unavailable",
		})
	}

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

	// Title is required for root interactions
	if req.ParentId == nil {
		if req.Title == nil || strings.TrimSpace(*req.Title) == "" {
			return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{
				Error: "title is required for root posts",
			})
		}
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

		d, rID, err := h.resolveParentHierarchy(ctx.Request().Context(), *req.ParentId)
		if err != nil {
			if errors.Is(err, database.ErrNotFound) {
				return ctx.JSON(http.StatusBadRequest, server.ErrorResponse{
					Error: "parent interaction not found",
				})
			}
			log.Error().Err(err).Str("parent_id", pIDStr).Msg("failed to lookup parent interaction")
			return ctx.JSON(http.StatusInternalServerError, server.ErrorResponse{
				Error: "failed to lookup parent interaction",
			})
		}
		depth = d
		rootIDStr = rID
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

	var rUUID *uuid.UUID
	if rootIDStr != nil {
		if parsed, err := uuid.Parse(*rootIDStr); err == nil {
			rUUID = &parsed
		}
	}
	h.recentInteractions.Store(newID, cachedInteraction{
		ID:     newID,
		RootID: rUUID,
		Depth:  depth,
	})

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

// EventsInteractions streams live interaction events to connected clients via Server-Sent Events (SSE).
func (h *Handler) EventsInteractions(ctx echo.Context, params server.EventsInteractionsParams) error {
	ctx.Response().Header().Set("Content-Type", "text/event-stream")
	ctx.Response().Header().Set("Cache-Control", "no-cache")
	ctx.Response().Header().Set("Connection", "keep-alive")
	ctx.Response().Header().Set("Access-Control-Allow-Origin", "*")
	ctx.Response().WriteHeader(http.StatusOK)
	ctx.Response().Flush()

	if h.publisher == nil {
		return nil
	}

	reqCtx := ctx.Request().Context()
	msgChan, cancelSub, err := h.publisher.SubscribeEvents(reqCtx, params.GroupId)
	if err != nil {
		log.Error().Err(err).Msg("failed to subscribe to NATS events for SSE")
		return nil
	}
	defer cancelSub()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-reqCtx.Done():
			return nil
		case <-heartbeat.C:
			if _, err := fmt.Fprintf(ctx.Response(), ":ping\n\n"); err != nil {
				return nil
			}
			ctx.Response().Flush()
		case msg, ok := <-msgChan:
			if !ok {
				return nil
			}
			if _, err := fmt.Fprintf(ctx.Response(), "data: %s\n\n", msg); err != nil {
				return nil
			}
			ctx.Response().Flush()
		}
	}
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

func (h *Handler) resolveParentHierarchy(ctx context.Context, parentID uuid.UUID) (int, *string, error) {
	if val, ok := h.recentInteractions.Load(parentID); ok {
		cached := val.(cachedInteraction)
		depth := cached.Depth + 1
		if cached.RootID != nil {
			rStr := cached.RootID.String()
			return depth, &rStr, nil
		}
		rStr := cached.ID.String()
		return depth, &rStr, nil
	}

	if h.repo == nil {
		pStr := parentID.String()
		return 1, &pStr, nil
	}

	parent, err := h.repo.GetByID(ctx, parentID)
	if err != nil {
		return 0, nil, err
	}

	depth := parent.Depth + 1
	if parent.RootID != nil {
		rStr := parent.RootID.String()
		return depth, &rStr, nil
	}

	rStr := parent.ID.String()
	return depth, &rStr, nil
}
