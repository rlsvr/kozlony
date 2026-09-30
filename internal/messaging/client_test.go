package messaging_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"kozlony/internal/config"
	"kozlony/internal/messaging"
	"kozlony/internal/messaging/commands"
	"kozlony/internal/messaging/events"
)

func TestNewClientNilConfig(t *testing.T) {
	client, err := messaging.NewClient(nil)
	if err == nil {
		t.Fatal("expected error when passing nil config, got nil")
	}
	if client != nil {
		t.Fatalf("expected nil client, got %v", client)
	}
}

func TestNewClientEmptyURL(t *testing.T) {
	cfg := &config.Config{
		NATSURL: "",
		AppName: "test-app",
	}

	client, err := messaging.NewClient(cfg)
	if err == nil {
		if client != nil {
			client.Close()
		}
		t.Fatal("expected error connecting with empty NATS URL")
	}
}

func TestEventSerialization(t *testing.T) {
	title := "Welcome Post"
	parentID := "01923f12-0000-7000-8000-000000000001"
	rootID := "01923f12-0000-7000-8000-000000000001"

	evt := events.InteractionCreatedEvent{
		Author:     "Alice",
		Body:       "Hello world message",
		CreatedAt:  time.Now().UTC(),
		Depth:      1,
		GroupID:    "general",
		ID:         "01923f12-0000-7000-8000-000000000002",
		ParentID:   &parentID,
		ReplyCount: 0,
		RootID:     &rootID,
		Title:      &title,
		Version:    1,
	}

	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	var decoded events.InteractionCreatedEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal event: %v", err)
	}

	if decoded.ID != evt.ID {
		t.Errorf("expected ID %s, got %s", evt.ID, decoded.ID)
	}
	if decoded.Author != evt.Author {
		t.Errorf("expected Author %s, got %s", evt.Author, decoded.Author)
	}
	if decoded.GroupID != evt.GroupID {
		t.Errorf("expected GroupID %s, got %s", evt.GroupID, decoded.GroupID)
	}
	if *decoded.Title != title {
		t.Errorf("expected Title %s, got %s", title, *decoded.Title)
	}
	if decoded.Version != 1 {
		t.Errorf("expected Version 1, got %d", decoded.Version)
	}

	// Test InteractionEditedEvent with version
	editedTitle := "Updated Title"
	editEvt := events.InteractionEditedEvent{
		Body:      "Updated body content",
		GroupID:   "general",
		ID:        "01923f12-0000-7000-8000-000000000002",
		Title:     &editedTitle,
		UpdatedAt: time.Now().UTC(),
		Version:   2,
	}

	editData, err := json.Marshal(editEvt)
	if err != nil {
		t.Fatalf("failed to marshal edit event: %v", err)
	}

	var decodedEdit events.InteractionEditedEvent
	if err := json.Unmarshal(editData, &decodedEdit); err != nil {
		t.Fatalf("failed to unmarshal edit event: %v", err)
	}
	if decodedEdit.Version != 2 {
		t.Errorf("expected edit version 2, got %d", decodedEdit.Version)
	}
}

func TestCommandSerialization(t *testing.T) {
	title := "New Root Post"
	cmd := commands.CreateInteractionCommand{
		Author:  "Bob",
		Body:    "Main content",
		GroupID: "dev",
		Title:   &title,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("failed to marshal command: %v", err)
	}

	var decoded commands.CreateInteractionCommand
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal command: %v", err)
	}

	if decoded.Author != cmd.Author {
		t.Errorf("expected Author %s, got %s", cmd.Author, decoded.Author)
	}
	if decoded.GroupID != cmd.GroupID {
		t.Errorf("expected GroupID %s, got %s", cmd.GroupID, decoded.GroupID)
	}
	if *decoded.Title != title {
		t.Errorf("expected Title %s, got %s", title, *decoded.Title)
	}
}

func TestPublishNilArguments(t *testing.T) {
	// Dummy client with nil connection to verify validation before network calls
	client := &messaging.Client{}

	if _, err := client.PublishInteractionCreated(context.Background(), nil); err == nil {
		t.Error("expected error publishing nil InteractionCreatedEvent")
	}

	if _, err := client.PublishInteractionEdited(context.Background(), nil); err == nil {
		t.Error("expected error publishing nil InteractionEditedEvent")
	}

	if _, err := client.PublishCreateInteractionCmd(context.Background(), "dev", nil); err == nil {
		t.Error("expected error publishing nil CreateInteractionCommand")
	}
}

func TestInitNilConfig(t *testing.T) {
	client, err := messaging.Init(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error when passing nil config, got nil")
	}
	if client != nil {
		t.Fatalf("expected nil client, got %v", client)
	}
}

func TestInitEmptyURL(t *testing.T) {
	cfg := &config.Config{
		NATSURL: "",
		AppName: "test-app",
	}

	client, err := messaging.Init(context.Background(), cfg)
	if err == nil {
		if client != nil {
			client.Close()
		}
		t.Fatal("expected error connecting with empty NATS URL in Init")
	}
}
