package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/nats-io/nats.go/jetstream"

	"kozlony/internal/api/handler"
	"kozlony/internal/api/server"
	"kozlony/internal/messaging/events"
)

type mockPublisher struct {
	lastEvent *events.InteractionCreatedEvent
}

func (m *mockPublisher) PublishInteractionCreated(_ context.Context, evt *events.InteractionCreatedEvent) (*jetstream.PubAck, error) {
	m.lastEvent = evt
	return &jetstream.PubAck{}, nil
}

func TestHealthEndpoints(t *testing.T) {
	e := echo.New()
	h := handler.New(nil, nil)

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

func TestCreateInteraction(t *testing.T) {
	e := echo.New()
	mockPub := &mockPublisher{}
	h := handler.New(mockPub, nil)

	title := "My first post"
	group := "dev"
	payload := server.CreateInteractionRequest{
		Author:  "Alice",
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

	if err := h.CreateInteraction(c); err != nil {
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
	if mockPub.lastEvent == nil {
		t.Fatal("expected event to be published, got nil")
	}
	if mockPub.lastEvent.Author != payload.Author {
		t.Errorf("expected published author %s, got %s", payload.Author, mockPub.lastEvent.Author)
	}
}

func TestCreateInteractionValidation(t *testing.T) {
	e := echo.New()
	h := handler.New(nil, nil)

	// Missing body
	payload := server.CreateInteractionRequest{
		Author: "Bob",
		Body:   "",
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/interactions", bytes.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := h.CreateInteraction(c); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 Bad Request, got %d", rec.Code)
	}
}
