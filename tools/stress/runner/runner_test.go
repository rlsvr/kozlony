package runner_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rlsvr/kozlony/tools/stress/runner"
)

const (
	fieldInteractions = "interactions"
	fieldDepth        = "depth"
	fieldVersion      = "version"
	fieldAuthor       = "author"
)

func TestConfigValidation(t *testing.T) {
	cfg := runner.DefaultConfig()
	require.NoError(t, cfg.Validate())

	invalidURL := cfg
	invalidURL.TargetURL = ""
	require.Error(t, invalidURL.Validate())

	invalidUsers := cfg
	invalidUsers.Users = 0
	require.Error(t, invalidUsers.Validate())

	invalidWorkers := cfg
	invalidWorkers.Workers = 0
	require.Error(t, invalidWorkers.Validate())

	tooManyWorkers := cfg
	tooManyWorkers.Workers = runner.MaxAllowedWorkers + 1
	require.Error(t, tooManyWorkers.Validate())

	invalidBodyNeg := cfg
	invalidBodyNeg.MinBody = -1
	require.Error(t, invalidBodyNeg.Validate())

	invalidBodyOrder := cfg
	invalidBodyOrder.MinBody = 500
	invalidBodyOrder.MaxBody = 100
	require.Error(t, invalidBodyOrder.Validate())

	invalidBodyTooLarge := cfg
	invalidBodyTooLarge.MaxBody = 70000
	require.Error(t, invalidBodyTooLarge.Validate())

	invalidRPS := cfg
	invalidRPS.RPS = -10
	require.Error(t, invalidRPS.Validate())

	invalidWeights := cfg
	invalidWeights.RootWeight = 0
	invalidWeights.ReplyWeight = 0
	invalidWeights.EditWeight = 0
	invalidWeights.ReadWeight = 0
	require.Error(t, invalidWeights.Validate())

	invalidWithClients := cfg
	invalidWithClients.WithClients = -1
	require.Error(t, invalidWithClients.Validate())

	tooManyWithClients := cfg
	tooManyWithClients.WithClients = 10001
	require.Error(t, tooManyWithClients.Validate())
}

func TestParseFlags(t *testing.T) {
	args := []string{
		"-url", "http://test-server:9090",
		"-users", "1000000",
		"-workers", "150",
		"-with-clients", "25",
		"-rps", "45.5",
		"-min-think", "500ms", // Deprecated flag handled gracefully
		"-max-think", "1500ms",
		"-min-body", "50",
		"-max-body", "500",
		"-duration", "10s",
		"-group", "custom-group",
	}

	cfg, err := runner.ParseFlags(args)
	require.NoError(t, err)
	assert.Equal(t, "http://test-server:9090", cfg.TargetURL)
	assert.Equal(t, 1000000, cfg.Users)
	assert.Equal(t, 150, cfg.Workers)
	assert.Equal(t, 25, cfg.WithClients)
	assert.InDelta(t, 45.5, cfg.RPS, 0.001)
	assert.Equal(t, 50, cfg.MinBody)
	assert.Equal(t, 500, cfg.MaxBody)
	assert.Equal(t, 10*time.Second, cfg.Duration)
	assert.Equal(t, "custom-group", cfg.GroupID)
}

func TestInteractionPool(t *testing.T) {
	pool := runner.NewInteractionPool(3)
	assert.Equal(t, 0, pool.Size())

	_, ok := pool.GetRandomParent()
	assert.False(t, ok)

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()
	id4 := uuid.New()

	pool.Add(runner.InteractionRef{ID: id1, Version: 1})
	pool.Add(runner.InteractionRef{ID: id2, Version: 1})
	pool.Add(runner.InteractionRef{ID: id3, Version: 1})
	assert.Equal(t, 3, pool.Size())

	// Re-adding existing ID updates without expanding
	pool.Add(runner.InteractionRef{ID: id2, Version: 2})
	assert.Equal(t, 3, pool.Size())

	// Eviction of oldest (id1)
	pool.Add(runner.InteractionRef{ID: id4, Version: 1})
	assert.Equal(t, 3, pool.Size())

	// Test UpdateVersion
	pool.UpdateVersion(id4, 5)
	item, ok := pool.GetRandomForEdit()
	assert.True(t, ok)
	assert.NotEqual(t, uuid.Nil, item.ID)

	// Random parent returns valid items
	parent, ok := pool.GetRandomParent()
	assert.True(t, ok)
	assert.NotEqual(t, uuid.Nil, parent.ID)
}

func TestInteractionPoolSeed(t *testing.T) {
	fakeID := uuid.New()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/interactions", r.URL.Path)
		assert.Equal(t, "test-group", r.URL.Query().Get("group_id"))

		resp := map[string]any{
			fieldInteractions: []map[string]any{
				{
					"id":         fakeID.String(),
					fieldDepth:   0,
					fieldVersion: 1,
					fieldAuthor:  "alice",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	pool := runner.NewInteractionPool(10)
	err := pool.SeedFromFeed(context.Background(), ts.URL, "test-group")
	require.NoError(t, err)
	assert.Equal(t, 1, pool.Size())

	parent, ok := pool.GetRandomParent()
	assert.True(t, ok)
	assert.Equal(t, fakeID, parent.ID)
}

func TestMetricsPercentiles(t *testing.T) {
	p50, p90, p99, maxLat := runner.LatencyPercentiles(nil)
	assert.Equal(t, time.Duration(0), p50)
	assert.Equal(t, time.Duration(0), p90)
	assert.Equal(t, time.Duration(0), p99)
	assert.Equal(t, time.Duration(0), maxLat)

	lats := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
	}
	p50, p90, p99, maxLat = runner.LatencyPercentiles(lats)
	assert.Equal(t, 30*time.Millisecond, p50)
	assert.Equal(t, 50*time.Millisecond, p90)
	assert.Equal(t, 50*time.Millisecond, p99)
	assert.Equal(t, 50*time.Millisecond, maxLat)
}

func TestMetricsRecordingAndReporting(t *testing.T) {
	m := runner.NewMetrics()
	m.Record("create_root", http.StatusCreated, 15*time.Millisecond, nil)
	m.Record("create_reply", http.StatusCreated, 10*time.Millisecond, nil)
	m.Record("edit", http.StatusOK, 5*time.Millisecond, nil)
	m.Record("edit", http.StatusConflict, 6*time.Millisecond, errors.New("conflict"))
	m.SetSSEClients(5)
	m.RecordSSEEvent()
	m.RecordSSEEvent()
	m.RecordSSEError()

	var buf bytes.Buffer
	m.PrintLive(&buf, 10)
	liveStr := buf.String()
	assert.Contains(t, liveStr, "rps=")
	assert.Contains(t, liveStr, "pool=10")
	assert.Contains(t, liveStr, "sse=5")

	buf.Reset()
	m.PrintSummary(&buf, 10)
	sumStr := buf.String()
	assert.Contains(t, sumStr, "STRESS TEST SUMMARY")
	assert.Contains(t, sumStr, "create_root")
	assert.Contains(t, sumStr, "create_reply")
	assert.Contains(t, sumStr, "HTTP 201")
	assert.Contains(t, sumStr, "HTTP 409")
	assert.Contains(t, sumStr, "--- Real-Time SSE Clients ---")
	assert.Contains(t, sumStr, "Events Received:")
}

func TestPressureController(t *testing.T) {
	// Mock HTTP server responding to create, reply, edit, read
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			created := map[string]any{
				"id":         uuid.New().String(),
				fieldDepth:   0,
				fieldVersion: 1,
				fieldAuthor:  "tester",
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(created)
		case http.MethodPut:
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "updated"})
		case http.MethodGet:
			if strings.Contains(r.URL.Path, "/thread") {
				_ = json.NewEncoder(w).Encode(map[string]any{fieldInteractions: []any{}})
			} else {
				_ = json.NewEncoder(w).Encode(map[string]any{fieldInteractions: []any{}})
			}
		}
	}))
	defer server.Close()

	cfg := runner.Config{
		TargetURL:      server.URL,
		Users:          3,
		RPS:            50,
		Duration:       100 * time.Millisecond,
		GroupID:        "root",
		ReportInterval: 50 * time.Millisecond,
		RootWeight:     25,
		ReplyWeight:    25,
		EditWeight:     25,
		ReadWeight:     25,
	}

	pool := runner.NewInteractionPool(100)
	metrics := runner.NewMetrics()
	controller := runner.NewPressureController(cfg, pool, metrics)

	var buf bytes.Buffer
	err := controller.Run(context.Background(), &buf)
	require.NoError(t, err)

	assert.Positive(t, pool.Size())
	assert.Contains(t, buf.String(), "STRESS TEST SUMMARY")
}

func TestPressureControllerWithClients(t *testing.T) {
	// Mock HTTP server responding to write traffic and streaming SSE events
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/interactions/events" {
			assert.Equal(t, "text/event-stream", r.Header.Get("Accept"))
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
				// Send mock event
				_, _ = w.Write([]byte("data: {\"id\":\"mock-event\"}\n\n"))
				f.Flush()
			}
			<-r.Context().Done()
			return
		}

		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			created := map[string]any{
				"id":         uuid.New().String(),
				fieldDepth:   0,
				fieldVersion: 1,
				fieldAuthor:  "tester",
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(created)
		case http.MethodPut:
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "updated"})
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{fieldInteractions: []any{}})
		}
	}))
	defer server.Close()

	cfg := runner.Config{
		TargetURL:      server.URL,
		Users:          2,
		Workers:        5,
		WithClients:    3,
		RPS:            50,
		Duration:       150 * time.Millisecond,
		GroupID:        "root",
		ReportInterval: 50 * time.Millisecond,
		RootWeight:     50,
		ReplyWeight:    50,
	}

	pool := runner.NewInteractionPool(50)
	metrics := runner.NewMetrics()
	controller := runner.NewPressureController(cfg, pool, metrics)

	var buf bytes.Buffer
	err := controller.Run(context.Background(), &buf)
	require.NoError(t, err)

	snap := metrics.Snapshot()
	assert.GreaterOrEqual(t, snap.SSEEvents, int64(3))
	assert.Contains(t, buf.String(), "--- Real-Time SSE Clients ---")
}
