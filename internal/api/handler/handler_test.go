package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/rlsvr/kozlony/internal/api/handler"
	"github.com/rlsvr/kozlony/internal/api/server"
	"github.com/rlsvr/kozlony/internal/database"
	"github.com/rlsvr/kozlony/internal/messaging/events"
)

const (
	testAuthorAlice = "Alice"
	testGroupRoot   = "root"
)

type mockPublisher struct {
	lastCreatedEvent *events.InteractionCreatedEvent
	lastEditedEvent  *events.InteractionEditedEvent
}

func (m *mockPublisher) PublishInteractionCreated(_ context.Context, evt *events.InteractionCreatedEvent) (*jetstream.PubAck, error) {
	m.lastCreatedEvent = evt
	return &jetstream.PubAck{}, nil
}

func (m *mockPublisher) PublishInteractionEdited(_ context.Context, evt *events.InteractionEditedEvent) (*jetstream.PubAck, error) {
	m.lastEditedEvent = evt
	return &jetstream.PubAck{}, nil
}

func (*mockPublisher) SubscribeEvents(_ context.Context, _ *string) (<-chan []byte, func(), error) {
	ch := make(chan []byte, 10)
	return ch, func() {}, nil
}

func (*mockPublisher) Ping(_ context.Context) error {
	return nil
}

type mockRepository struct {
	items map[uuid.UUID]*database.Interaction
}

func newMockRepository() *mockRepository {
	return &mockRepository{items: make(map[uuid.UUID]*database.Interaction)}
}

func (*mockRepository) Ping(_ context.Context) error {
	return nil
}

func (m *mockRepository) Insert(_ context.Context, item *database.Interaction) error {
	m.items[item.ID] = item
	return nil
}

func (m *mockRepository) InsertBatch(_ context.Context, items []*database.Interaction) error {
	for _, item := range items {
		m.items[item.ID] = item
	}
	return nil
}

func (m *mockRepository) Update(_ context.Context, id uuid.UUID, expectedVersion int, title *string, body string) (*database.Interaction, error) {
	item, ok := m.items[id]
	if !ok {
		return nil, database.ErrNotFound
	}
	if item.Version != expectedVersion {
		return nil, database.ErrVersionConflict
	}
	item.Title = title
	item.Body = body
	item.Version++
	now := time.Now().UTC()
	item.UpdatedAt = &now
	return item, nil
}

func (m *mockRepository) GetByID(_ context.Context, id uuid.UUID) (*database.Interaction, error) {
	item, ok := m.items[id]
	if !ok {
		return nil, database.ErrNotFound
	}
	return item, nil
}

func (m *mockRepository) GetThread(_ context.Context, rootID uuid.UUID) ([]*database.Interaction, error) {
	var thread []*database.Interaction
	for _, item := range m.items {
		if item.ID == rootID || (item.RootID != nil && *item.RootID == rootID) {
			thread = append(thread, item)
		}
	}
	return thread, nil
}

func (m *mockRepository) ListFeed(_ context.Context, groupID string, _ int, _ *time.Time) ([]*database.Interaction, error) {
	var feed []*database.Interaction
	for _, item := range m.items {
		if item.GroupID == groupID && item.ParentID == nil {
			feed = append(feed, item)
		}
	}
	return feed, nil
}

func TestHealthEndpoints(t *testing.T) {
	e := echo.New()
	pub := &mockPublisher{}
	repo := newMockRepository()
	h := handler.New(pub, repo)

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

func TestCreateInteraction(t *testing.T) {
	e := echo.New()
	mockPub := &mockPublisher{}
	h := handler.New(mockPub, nil)

	title := "My first post"
	group := "dev"
	payload := server.CreateInteractionRequest{
		Author:  testAuthorAlice,
		Body:    "Hello world!",
		GroupId: &group,
		Title:   &title,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/interactions", bytes.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := h.CreateInteraction(c, server.CreateInteractionParams{}); err != nil {
		t.Fatalf("unexpected error from CreateInteraction: %v", err)
	}

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201 Created, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var created server.Interaction
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if created.Author != payload.Author {
		t.Errorf("expected author %s, got %s", payload.Author, created.Author)
	}
	if created.GroupId != group {
		t.Errorf("expected group %s, got %s", group, created.GroupId)
	}
	if created.Version != 1 {
		t.Errorf("expected version 1, got %d", created.Version)
	}
	if mockPub.lastCreatedEvent == nil {
		t.Fatal("expected event to be published, got nil")
	}
	if mockPub.lastCreatedEvent.Author != payload.Author {
		t.Errorf("expected published author %s, got %s", payload.Author, mockPub.lastCreatedEvent.Author)
	}
	if mockPub.lastCreatedEvent.Version != 1 {
		t.Errorf("expected event version 1, got %d", mockPub.lastCreatedEvent.Version)
	}
}

func TestCreateInteraction_IdempotencyKey(t *testing.T) {
	e := echo.New()
	mockPub := &mockPublisher{}
	repo := newMockRepository()
	h := handler.New(mockPub, repo)

	idempotencyKey := "01923f12-1111-7000-8000-000000000001"
	title := "Idempotent Title"
	payload := server.CreateInteractionRequest{
		Author: "Charlie",
		Title:  &title,
		Body:   "Idempotent post",
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/interactions", bytes.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	req.Header.Set("Idempotency-Key", idempotencyKey)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	params := server.CreateInteractionParams{IdempotencyKey: &idempotencyKey}
	if err := h.CreateInteraction(c, params); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", rec.Code)
	}

	// Save into repo to simulate drainer write
	parsedUUID := uuid.MustParse(idempotencyKey)
	_ = repo.Insert(context.Background(), &database.Interaction{
		ID:        parsedUUID,
		GroupID:   testGroupRoot,
		Author:    payload.Author,
		Body:      payload.Body,
		CreatedAt: time.Now().UTC(),
		Version:   1,
	})

	// Retry request with same Idempotency-Key
	reqRetry := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/interactions", bytes.NewReader(body))
	reqRetry.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	reqRetry.Header.Set("Idempotency-Key", idempotencyKey)
	recRetry := httptest.NewRecorder()
	cRetry := e.NewContext(reqRetry, recRetry)

	if err := h.CreateInteraction(cRetry, params); err != nil {
		t.Fatalf("unexpected error on retry: %v", err)
	}
	if recRetry.Code != http.StatusOK {
		t.Errorf("expected 200 OK for already processed idempotency key, got %d", recRetry.Code)
	}
}

func TestCreateInteractionValidation(t *testing.T) {
	e := echo.New()
	h := handler.New(nil, nil)

	tests := []struct {
		name    string
		payload server.CreateInteractionRequest
	}{
		{
			name: "Missing author",
			payload: server.CreateInteractionRequest{
				Author: "",
				Body:   "Valid body",
			},
		},
		{
			name: "Author exceeds 100 chars",
			payload: server.CreateInteractionRequest{
				Author: strings.Repeat("a", 101),
				Body:   "Valid body",
			},
		},
		{
			name: "Missing body",
			payload: server.CreateInteractionRequest{
				Author: testAuthorAlice,
				Body:   "",
			},
		},
		{
			name: "Body exceeds 65535 chars",
			payload: server.CreateInteractionRequest{
				Author: testAuthorAlice,
				Body:   strings.Repeat("b", 65536),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.payload)
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/interactions", bytes.NewReader(body))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			if err := h.CreateInteraction(c, server.CreateInteractionParams{}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rec.Code != http.StatusBadRequest {
				t.Errorf("expected 400 Bad Request, got %d", rec.Code)
			}
		})
	}
}

func TestUpdateInteraction(t *testing.T) {
	e := echo.New()
	mockPub := &mockPublisher{}
	repo := newMockRepository()
	h := handler.New(mockPub, repo)

	postID := uuid.Must(uuid.NewV7())
	title := "Initial Title"
	_ = repo.Insert(context.Background(), &database.Interaction{
		ID:        postID,
		GroupID:   testGroupRoot,
		Title:     &title,
		Body:      "Initial Body",
		Author:    testAuthorAlice,
		CreatedAt: time.Now().UTC(),
		Version:   1,
	})

	// 1. Successful update
	newTitle := "Updated Title"
	updateReq := server.UpdateInteractionRequest{
		Title:   &newTitle,
		Body:    "Updated Body Content",
		Version: 1,
	}
	body, _ := json.Marshal(updateReq)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/v1/interactions/"+postID.String(), bytes.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := h.UpdateInteraction(c, postID); err != nil {
		t.Fatalf("unexpected error updating interaction: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var updated server.Interaction
	_ = json.Unmarshal(rec.Body.Bytes(), &updated)
	if updated.Version != 2 {
		t.Errorf("expected incremented version 2, got %d", updated.Version)
	}
	if updated.Body != updateReq.Body {
		t.Errorf("expected body %s, got %s", updateReq.Body, updated.Body)
	}
	if mockPub.lastEditedEvent == nil {
		t.Fatal("expected InteractionEditedEvent to be published")
	}
	if mockPub.lastEditedEvent.Version != 2 {
		t.Errorf("expected event version 2, got %d", mockPub.lastEditedEvent.Version)
	}

	// 2. Version conflict (sending stale version 1 again)
	reqConflict := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/v1/interactions/"+postID.String(), bytes.NewReader(body))
	reqConflict.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	recConflict := httptest.NewRecorder()
	cConflict := e.NewContext(reqConflict, recConflict)

	if err := h.UpdateInteraction(cConflict, postID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recConflict.Code != http.StatusConflict {
		t.Errorf("expected 409 Conflict, got %d", recConflict.Code)
	}

	// 3. Not found
	nonExistentID := uuid.Must(uuid.NewV7())
	reqNotFound := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/v1/interactions/"+nonExistentID.String(), bytes.NewReader(body))
	reqNotFound.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	recNotFound := httptest.NewRecorder()
	cNotFound := e.NewContext(reqNotFound, recNotFound)

	if err := h.UpdateInteraction(cNotFound, nonExistentID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recNotFound.Code != http.StatusNotFound {
		t.Errorf("expected 404 Not Found, got %d", recNotFound.Code)
	}
}

func TestEventsInteractions(t *testing.T) {
	e := echo.New()
	pub := &mockPublisher{}
	repo := newMockRepository()
	h := handler.New(pub, repo)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/interactions/events", nil)
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := h.EventsInteractions(c, server.EventsInteractionsParams{})
	if err != nil {
		t.Fatalf("unexpected error from EventsInteractions: %v", err)
	}

	if !strings.Contains(rec.Header().Get("Content-Type"), "text/event-stream") {
		t.Errorf("expected Content-Type text/event-stream, got %s", rec.Header().Get("Content-Type"))
	}
}

func TestCreateInteraction_RootTitleRequired(t *testing.T) {
	e := echo.New()
	pub := &mockPublisher{}
	repo := newMockRepository()
	h := handler.New(pub, repo)

	// Missing title on root interaction
	payload := `{"author":"Alice","body":"Missing title body"}`
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/interactions", strings.NewReader(payload))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := h.CreateInteraction(c, server.CreateInteractionParams{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 Bad Request, got %d", rec.Code)
	}
}

func TestGetReadyz_Healthy(t *testing.T) {
	e := echo.New()
	pub := &mockPublisher{}
	repo := newMockRepository()
	h := handler.New(pub, repo)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/readyz", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := h.GetReadyz(c); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec.Code)
	}
}

func TestCreateInteraction_NestedReplyDepth(t *testing.T) {
	e := echo.New()
	pub := &mockPublisher{}
	repo := newMockRepository()
	h := handler.New(pub, repo)

	// 1. Root post
	rootID := uuid.Must(uuid.NewV7())
	title := "Root Post"
	_ = repo.Insert(context.Background(), &database.Interaction{
		ID:        rootID,
		GroupID:   testGroupRoot,
		Title:     &title,
		Body:      "Root body",
		Author:    "Alice",
		CreatedAt: time.Now().UTC(),
		Depth:     0,
		Version:   1,
	})

	// 2. Direct reply to root post (depth 1)
	reply1ID := uuid.Must(uuid.NewV7())
	_ = repo.Insert(context.Background(), &database.Interaction{
		ID:        reply1ID,
		GroupID:   testGroupRoot,
		RootID:    &rootID,
		ParentID:  &rootID,
		Body:      "Reply 1",
		Author:    "Bob",
		CreatedAt: time.Now().UTC(),
		Depth:     1,
		Version:   1,
	})

	// 3. Nested reply to reply 1 (should have depth 2 and rootID = rootID)
	payload := fmt.Sprintf(`{"author":"Charlie","body":"Reply 2","parent_id":"%s"}`, reply1ID)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/interactions", strings.NewReader(payload))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := h.CreateInteraction(c, server.CreateInteractionParams{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Code != http.StatusOK && rec.Code != http.StatusCreated {
		t.Fatalf("expected 200 or 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if pub.lastCreatedEvent == nil {
		t.Fatal("expected event to be published")
	}
	if pub.lastCreatedEvent.Depth != 2 {
		t.Errorf("expected depth 2, got %d", pub.lastCreatedEvent.Depth)
	}
	if pub.lastCreatedEvent.RootID == nil || *pub.lastCreatedEvent.RootID != rootID.String() {
		t.Errorf("expected rootID %s, got %v", rootID.String(), pub.lastCreatedEvent.RootID)
	}
}
