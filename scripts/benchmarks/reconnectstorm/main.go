package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	targetURL := flag.String("url", "http://localhost:8080", "Base URL")
	concurrency := flag.Int("clients", 5000, "Number of clients to connect simultaneously")
	timeout := flag.Duration("timeout", 15*time.Second, "Max time to wait for storm")
	flag.Parse()

	transport := &http.Transport{
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency * 2,
		DisableKeepAlives:   false,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   0, // keep stream open
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var (
		connected atomic.Int64
		failed    atomic.Int64
		wg        sync.WaitGroup
	)

	start := time.Now()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, *targetURL+"/v1/interactions/events?group_id=root", nil)
			if err != nil {
				failed.Add(1)
				return
			}
			req.Header.Set("Accept", "text/event-stream")

			resp, err := client.Do(req)
			if err != nil {
				failed.Add(1)
				return
			}
			if resp.StatusCode == http.StatusOK {
				connected.Add(1)
				// Hold open until test terminates
				<-ctx.Done()
			} else {
				failed.Add(1)
			}
			_ = resp.Body.Close()
		}(i)
	}

	// Wait until either all connected or timeout fires
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var totalTime time.Duration
	for {
		select {
		case <-ctx.Done():
			totalTime = *timeout
			goto DONE
		case <-ticker.C:
			conn := connected.Load()
			fail := failed.Load()
			if conn+fail >= int64(*concurrency) {
				totalTime = time.Since(start)
				cancel()
				goto DONE
			}
		}
	}

DONE:
	wg.Wait()
	conn := connected.Load()
	fail := failed.Load()
	sec := totalTime.Seconds()
	if sec <= 0 {
		sec = 0.001
	}
	rate := float64(conn) / sec

	fmt.Printf("Total Attempted: %d\n", *concurrency)
	fmt.Printf("Established:     %d\n", conn)
	fmt.Printf("Failed:          %d\n", fail)
	fmt.Printf("Elapsed:         %.2fs\n", sec)
	fmt.Printf("Handshake Rate:  %.1f handshakes/sec\n", rate)
}
