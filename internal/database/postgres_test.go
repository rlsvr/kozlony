package database_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"kozlony/internal/database"
)

func TestConnectInvalidURL(t *testing.T) {
	_, err := database.Connect(context.Background(), "invalid://localhost:not-a-port")
	if err == nil {
		t.Fatal("expected error connecting with invalid database URL, got nil")
	}
}

func TestInsertBatchEmpty(t *testing.T) {
	repo := database.NewInteractionRepository(nil)
	if err := repo.InsertBatch(context.Background(), nil); err != nil {
		t.Fatalf("expected nil error for empty batch, got: %v", err)
	}
	if err := repo.InsertBatch(context.Background(), []*database.Interaction{}); err != nil {
		t.Fatalf("expected nil error for empty slice, got: %v", err)
	}
}

func TestInteractionModel(t *testing.T) {
	id := uuid.Must(uuid.NewV7())
	title := "Test Interaction"
	now := time.Now().UTC()

	item := database.Interaction{
		ID:         id,
		GroupID:    "root",
		Title:      &title,
		Body:       "Body text",
		Author:     "Bob",
		CreatedAt:  now,
		ReplyCount: 0,
		Depth:      0,
		Version:    1,
	}

	if item.ID != id {
		t.Errorf("expected ID %v, got %v", id, item.ID)
	}
	if item.GroupID != "root" {
		t.Errorf("expected group root, got %s", item.GroupID)
	}
	if *item.Title != title {
		t.Errorf("expected title %s, got %s", title, *item.Title)
	}
	if item.Version != 1 {
		t.Errorf("expected version 1, got %d", item.Version)
	}
}
