package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

func main() {
	targetURL := flag.String("url", "http://localhost:8080", "Base URL")
	numProbes := flag.Int("probes", 50, "Number of probe messages to send")
	interval := flag.Duration("interval", 100*time.Millisecond, "Interval between probes")
	flag.Parse()

	// 1. Establish SSE listener
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sseReq, err := http.NewRequestWithContext(ctx, http.MethodGet, *targetURL+"/v1/interactions/events?group_id=root", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create SSE request: %v\n", err)
		os.Exit(1)
	}
	sseReq.Header.Set("Accept", "text/event-stream")

	sseClient := &http.Client{Timeout: 0}
	sseResp, err := sseClient.Do(sseReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to SSE: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = sseResp.Body.Close()
	}()

	if sseResp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "SSE returned non-200: %d\n", sseResp.StatusCode)
		os.Exit(1)
	}

	var mu sync.Mutex
	probeDelays := make([]time.Duration, 0, *numProbes)
	pendingProbes := make(map[string]time.Time)

	// Reader goroutine
	ready := make(chan struct{})
	go func() {
		close(ready)
		scanner := bufio.NewScanner(sseResp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			now := time.Now()

			// Check if this payload contains probe token
			if strings.Contains(data, "probe_nano:") {
				idx := strings.Index(data, "probe_nano:")
				sub := data[idx+len("probe_nano:"):]
				endIdx := strings.IndexAny(sub, " ,\"}\n")
				var sentNanoStr string
				if endIdx != -1 {
					sentNanoStr = sub[:endIdx]
				} else {
					sentNanoStr = sub
				}
				if sentNano, err := strconv.ParseInt(sentNanoStr, 10, 64); err == nil {
					delay := now.Sub(time.Unix(0, sentNano))
					mu.Lock()
					probeDelays = append(probeDelays, delay)
					mu.Unlock()
				}
			}
		}
	}()

	<-ready
	time.Sleep(500 * time.Millisecond) // ensure subscription is registered in broker

	httpClient := &http.Client{Timeout: 5 * time.Second}

	// 2. Fire probes
	for i := 0; i < *numProbes; i++ {
		now := time.Now()
		sentNano := now.UnixNano()
		payload := map[string]any{
			"group_id": "root",
			"author":   "e2e-probe",
			"title":    fmt.Sprintf("Probe #%d", i),
			"body":     fmt.Sprintf("E2E latency probe_nano:%d", sentNano),
		}
		data, _ := json.Marshal(payload)
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, *targetURL+"/v1/interactions", bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", uuid.New().String())

		mu.Lock()
		pendingProbes[fmt.Sprintf("%d", sentNano)] = now
		mu.Unlock()

		resp, err := httpClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		time.Sleep(*interval)
	}

	// Wait 2s for all probes to arrive at the SSE listener
	time.Sleep(2 * time.Second)
	cancel()

	mu.Lock()
	defer mu.Unlock()

	if len(probeDelays) == 0 {
		fmt.Println("No probe events received on SSE stream.")
		return
	}

	sort.Slice(probeDelays, func(i, j int) bool {
		return probeDelays[i] < probeDelays[j]
	})

	p50 := probeDelays[len(probeDelays)*50/100]
	p90 := probeDelays[len(probeDelays)*90/100]
	p99 := probeDelays[len(probeDelays)*99/100]
	minLatency := probeDelays[0]
	maxLatency := probeDelays[len(probeDelays)-1]

	fmt.Printf("Received: %d/%d probes\n", len(probeDelays), *numProbes)
	fmt.Printf("Min:      %.2f ms\n", float64(minLatency.Microseconds())/1000.0)
	fmt.Printf("P50:      %.2f ms\n", float64(p50.Microseconds())/1000.0)
	fmt.Printf("P90:      %.2f ms\n", float64(p90.Microseconds())/1000.0)
	fmt.Printf("P99:      %.2f ms\n", float64(p99.Microseconds())/1000.0)
	fmt.Printf("Max:      %.2f ms\n", float64(maxLatency.Microseconds())/1000.0)
}
