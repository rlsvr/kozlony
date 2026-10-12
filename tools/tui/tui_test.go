package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rlsvr/kozlony/tools/status"
)

func TestSamplerJetStream(t *testing.T) {
	// Tests live sampling against the running local docker compose NATS container
	sampler := NewSampler(0, "", "http://localhost:8223")
	defer sampler.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	snap := sampler.Sample(ctx)

	require.True(t, snap.JetStream.Connected, "expected JetStream to be connected on http://localhost:8223")
	assert.Equal(t, "BOARD", snap.JetStream.StreamName)
	assert.Positive(t, snap.JetStream.TotalMessages, "expected BOARD stream to have messages")
	assert.Positive(t, snap.JetStream.LastSeq)
	assert.Equal(t, "board-drainer", snap.JetStream.Drainer.Name)
	assert.Equal(t, "board-counters", snap.JetStream.Counter.Name)
	assert.GreaterOrEqual(t, snap.JetStream.Drainer.WaitingWorkers, 0)
}

func TestRenderJetStreamCard(t *testing.T) {
	model := NewModel("/nonexistent", "http://localhost:8080", nil)
	model.width = 120
	model.sysSnap.JetStream = JetStreamStats{
		Connected:     true,
		StreamName:    "BOARD",
		TotalMessages: 3080324,
		TotalBytes:    2714581610,
		LastSeq:       3080324,
		Drainer: ConsumerStats{
			Name:           "board-drainer",
			Pending:        0,
			AckPending:     0,
			Redelivered:    0,
			WaitingWorkers: 8,
			StreamSeq:      3080317,
			AckFloorSeq:    3080317,
		},
		Counter: ConsumerStats{
			Name:           "board-counters",
			Pending:        0,
			AckPending:     0,
			Redelivered:    0,
			WaitingWorkers: 8,
			StreamSeq:      3080324,
			AckFloorSeq:    3080324,
		},
	}

	card := renderJetStreamCard(model, 110)
	assert.Contains(t, card, "JETSTREAM & DRAINER PIPELINE TELEMETRY")
	assert.Contains(t, card, "BOARD")
	assert.Contains(t, card, "board-drainer")
	assert.Contains(t, card, "board-counters")
	assert.Contains(t, card, "HEALTHY")
	assert.Contains(t, card, "Drainer Progress:")
}

func TestModelViewRendersAllCards(t *testing.T) {
	model := NewModel("/nonexistent", "http://localhost:8080", nil)
	model.width = 120
	model.height = 40
	model.metricsSnap = status.Snapshot{
		TotalRequests: 100,
	}

	view := model.View()
	assert.Contains(t, view, "⚡ KOZLONY TUI STACK & LOAD MONITOR")
	assert.Contains(t, view, "WORKLOAD & THROUGHPUT")
	assert.Contains(t, view, "JETSTREAM & DRAINER PIPELINE TELEMETRY")
	assert.Contains(t, view, "SYSTEM RESOURCES")
	assert.Contains(t, view, "DATABASE & HIERARCHICAL CONVERSATION STATE")
}
