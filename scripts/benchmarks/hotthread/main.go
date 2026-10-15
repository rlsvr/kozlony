package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Result struct {
	TotalRequests int64
	Successes     int64
	Errors        int64
	Deadlocks     int64
	Latencies     []time.Duration
}

func main() {
	targetURL := flag.String("url", "http://localhost:8080", "Base URL")
	duration := flag.Duration("duration", 15*time.Second, "Test duration")
	workers := flag.Int("workers", 200, "Worker pool size")
	flag.Parse()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        *workers * 2,
			MaxIdleConnsPerHost: *workers * 2,
		},
		Timeout: 10 * time.Second,
	}

	// 1. Create a single root post to be the "hot root"
	rootPayload := map[string]any{
		"group_id": "root",
		"author":   "benchmark-runner",
		"title":    "The Ultra Hot Thread",
		"body":     "Contention root target post",
	}
	rootData, _ := json.Marshal(rootPayload)
	req, _ := http.NewRequest(http.MethodPost, *targetURL+"/v1/interactions", bytes.NewReader(rootData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", uuid.New().String())

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create hot root: %v\n", err)
		os.Exit(1)
	}
	var created struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&created)
	_ = resp.Body.Close()

	if created.ID == "" {
		fmt.Fprintf(os.Stderr, "Empty ID returned for root post\n")
		os.Exit(1)
	}
	rootID := created.ID
	fmt.Printf("Created Hot Root Thread ID: %s\n", rootID)

	// 2. Hammer this single thread with 100% replies
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var (
		totalReq atomic.Int64
		success  atomic.Int64
		errors   atomic.Int64
		latMu    sync.Mutex
		lats     []time.Duration
	)

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localLats := make([]time.Duration, 0, 1000)
			for {
				select {
				case <-ctx.Done():
					latMu.Lock()
					lats = append(lats, localLats...)
					latMu.Unlock()
					return
				default:
				}

				payload := map[string]any{
					"group_id":  "root",
					"parent_id": rootID,
					"author":    fmt.Sprintf("user-%d", workerID),
					"body":      "Hammering hot thread with concurrent replies!",
				}
				data, _ := json.Marshal(payload)
				r, _ := http.NewRequestWithContext(ctx, http.MethodPost, *targetURL+"/v1/interactions", bytes.NewReader(data))
				r.Header.Set("Content-Type", "application/json")
				r.Header.Set("Idempotency-Key", uuid.New().String())

				start := time.Now()
				res, err := client.Do(r)
				dur := time.Since(start)
				totalReq.Add(1)

				if err == nil {
					if res.StatusCode >= 200 && res.StatusCode < 300 {
						success.Add(1)
						localLats = append(localLats, dur)
					} else {
						errors.Add(1)
					}
					_ = res.Body.Close()
				} else {
					errors.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()

	totalSec := duration.Seconds()
	achievedRPS := float64(success.Load()) / totalSec
	fmt.Printf("Duration:        %.1fs\n", totalSec)
	fmt.Printf("Total Requests:  %d\n", totalReq.Load())
	fmt.Printf("Successes:       %d\n", success.Load())
	fmt.Printf("Errors:          %d\n", errors.Load())
	fmt.Printf("Achieved RPS:    %.2f req/s\n", achievedRPS)
}
