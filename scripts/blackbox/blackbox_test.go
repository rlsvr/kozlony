package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestBlackboxLive(t *testing.T) {
	apiURL := "http://localhost:8080"
	natsURL := "nats://localhost:4222"

	// Probe if the server is running; skip if offline so make test passes in offline CI environments
	ctxCheck, cancelCheck := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelCheck()

	req, err := http.NewRequestWithContext(ctxCheck, http.MethodGet, apiURL+"/v1/healthz", nil)
	if err != nil {
		t.Skipf("cannot create healthz request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf("skipping live blackbox test: server not running on %s (%v)", apiURL, err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Skipf("skipping live blackbox test: server returned status %d", resp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg := config{
		apiURL:  apiURL,
		natsURL: natsURL,
		groupID: "bb-test-" + uuid.New().String()[:8],
	}

	if err := runTests(ctx, cfg); err != nil {
		t.Fatalf("blackbox test suite failed: %v", err)
	}
}
