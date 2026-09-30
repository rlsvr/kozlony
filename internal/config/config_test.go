package config_test

import (
	"context"
	"testing"
	"time"

	"kozlony/internal/config"
)

func TestConfigLoadDefaults(t *testing.T) {
	cfg, err := config.Load(context.Background())
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if cfg.AppName != "kozlony" {
		t.Errorf("expected AppName kozlony, got %s", cfg.AppName)
	}
	if cfg.Addr != ":8080" {
		t.Errorf("expected Addr :8080, got %s", cfg.Addr)
	}
	if cfg.NATSStreamName != "BOARD" {
		t.Errorf("expected Stream BOARD, got %s", cfg.NATSStreamName)
	}
	if cfg.MaxCachedInteractions != 500000 {
		t.Errorf("expected MaxCachedInteractions 500000, got %d", cfg.MaxCachedInteractions)
	}
}

func TestConfigLoadEnvOverride(t *testing.T) {
	t.Setenv("ADDR", ":9090")
	t.Setenv("NATS_STREAM_NAME", "CUSTOM_BOARD")
	t.Setenv("DRAIN_FLUSH_INTERVAL", "100ms")

	cfg, err := config.Load(context.Background())
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if cfg.Addr != ":9090" {
		t.Errorf("expected Addr :9090, got %s", cfg.Addr)
	}
	if cfg.NATSStreamName != "CUSTOM_BOARD" {
		t.Errorf("expected Stream CUSTOM_BOARD, got %s", cfg.NATSStreamName)
	}
	if cfg.DrainFlushInterval != 100*time.Millisecond {
		t.Errorf("expected 100ms, got %v", cfg.DrainFlushInterval)
	}
}
