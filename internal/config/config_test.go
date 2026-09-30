package config_test

import (
	"context"
	"testing"
	"time"

	"kozlony/internal/config"
)

func TestConfigLoadDefaults(t *testing.T) {
	t.Setenv("LOG_LEVEL", "info")
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
	if cfg.NATSConcurrentSubscribers != 32 {
		t.Errorf("expected NATSConcurrentSubscribers 32, got %d", cfg.NATSConcurrentSubscribers)
	}
	if cfg.NATSStreamRetention != 336*time.Hour {
		t.Errorf("expected NATSStreamRetention 336h, got %v", cfg.NATSStreamRetention)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected LogLevel info, got %s", cfg.LogLevel)
	}
	if !cfg.PrettyLogging {
		t.Errorf("expected PrettyLogging true by default, got %v", cfg.PrettyLogging)
	}
}

func TestConfigLoadEnvOverride(t *testing.T) {
	t.Setenv("ADDR", ":9090")
	t.Setenv("NATS_STREAM_NAME", "CUSTOM_BOARD")
	t.Setenv("DRAIN_FLUSH_INTERVAL", "100ms")
	t.Setenv("NATS_CONCURRENT_SUBSCRIBERS", "64")
	t.Setenv("NATS_STREAM_RETENTION", "720h")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("PRETTY_LOGGING", "false")

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
	if cfg.NATSConcurrentSubscribers != 64 {
		t.Errorf("expected 64, got %d", cfg.NATSConcurrentSubscribers)
	}
	if cfg.NATSStreamRetention != 720*time.Hour {
		t.Errorf("expected 720h, got %v", cfg.NATSStreamRetention)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected LogLevel debug, got %s", cfg.LogLevel)
	}
	if cfg.PrettyLogging {
		t.Errorf("expected PrettyLogging false, got %v", cfg.PrettyLogging)
	}
}
