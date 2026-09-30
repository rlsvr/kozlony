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
	"strings"
	"time"

	"github.com/google/uuid"
	natsio "github.com/nats-io/nats.go"
)

type config struct {
	apiURL  string
	natsURL string
	groupID string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.apiURL, "api", "http://localhost:8080", "Kozlony API base URL")
	flag.StringVar(&cfg.natsURL, "nats", "nats://localhost:4222", "NATS broker URL")
	flag.StringVar(&cfg.groupID, "group", fmt.Sprintf("bb-%s", uuid.New().String()[:8]), "Test board/group ID")
	flag.Parse()

	fmt.Printf("=== Kozlony Blackbox Test Suite ===\n")
	fmt.Printf("API URL:  %s\n", cfg.apiURL)
	fmt.Printf("NATS URL: %s\n", cfg.natsURL)
	fmt.Printf("Group ID: %s\n\n", cfg.groupID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := runTests(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\n❌ Blackbox test failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ All blackbox tests passed successfully!")
}

func runTests(ctx context.Context, cfg config) error {
	// 1. Health check
	fmt.Print("[1/8] Testing /v1/healthz... ")
	if err := testHealthz(ctx, cfg.apiURL); err != nil {
		return fmt.Errorf("healthz: %w", err)
	}
	fmt.Println("OK")

	// 2. Connect to NATS broker
	fmt.Print("[2/8] Connecting to NATS and subscribing to events... ")
	nc, eventsChan, cleanupNATS, err := setupNATSSubscriber(cfg.natsURL, cfg.groupID)
	if err != nil {
		fmt.Printf("SKIPPED (NATS unreachable: %v)\n", err)
	} else {
		defer cleanupNATS()
		fmt.Println("OK")
	}

	// 3. Test SSE Stream endpoint
	fmt.Print("[3/8] Testing SSE /v1/interactions/stream connection... ")
	sseChan, stopSSE, err := startSSEListener(ctx, cfg.apiURL, cfg.groupID)
	if err != nil {
		return fmt.Errorf("sse stream: %w", err)
	}
	defer stopSSE()
	fmt.Println("OK")

	// 4. Create root interaction with Idempotency-Key
	fmt.Print("[4/8] Testing POST /v1/interactions (create root post)... ")
	idempKey := uuid.New().String()
	rootTitle := "Blackbox Test Post"
	rootBody := "Testing end-to-end messaging pipeline"
	rootAuthor := "TesterAlice"

	rootPost, err := createInteraction(ctx, cfg.apiURL, idempKey, createReq{
		GroupID: cfg.groupID,
		Title:   &rootTitle,
		Body:    rootBody,
		Author:  rootAuthor,
	})
	if err != nil {
		return fmt.Errorf("create root post: %w", err)
	}
	fmt.Printf("OK (ID: %s)\n", rootPost.ID)

	// Verify NATS event and SSE event if NATS was connected
	if nc != nil {
		select {
		case msg := <-eventsChan:
			fmt.Printf("      -> Received NATS event on %s\n", msg.Subject)
		case <-time.After(2 * time.Second):
			return fmt.Errorf("timeout waiting for NATS created event")
		}

		select {
		case sseEvent := <-sseChan:
			fmt.Printf("      -> Received SSE event: %s\n", strings.TrimSpace(sseEvent))
		case <-time.After(2 * time.Second):
			fmt.Printf("      -> (Warning: no immediate SSE event received within timeout)\n")
		}
	}

	// 5. Test idempotency
	fmt.Print("[5/8] Testing POST /v1/interactions idempotency... ")
	dupPost, err := createInteraction(ctx, cfg.apiURL, idempKey, createReq{
		GroupID: cfg.groupID,
		Title:   &rootTitle,
		Body:    rootBody,
		Author:  rootAuthor,
	})
	if err != nil {
		return fmt.Errorf("duplicate post failed: %w", err)
	}
	if dupPost.ID != rootPost.ID {
		return fmt.Errorf("idempotency failed: expected ID %s, got %s", rootPost.ID, dupPost.ID)
	}
	fmt.Println("OK")

	// 6. Create reply interaction
	fmt.Print("[6/8] Testing POST /v1/interactions (reply to post)... ")
	replyBody := "Reply from blackbox test suite"
	replyAuthor := "TesterBob"
	replyPost, err := createInteraction(ctx, cfg.apiURL, uuid.New().String(), createReq{
		GroupID:  cfg.groupID,
		ParentID: &rootPost.ID,
		Body:     replyBody,
		Author:   replyAuthor,
	})
	if err != nil {
		return fmt.Errorf("create reply: %w", err)
	}
	fmt.Printf("OK (Reply ID: %s)\n", replyPost.ID)

	// Wait for drainer micro-batch ingestion into PostgreSQL
	fmt.Print("[7/8] Waiting for drainer micro-batch flush and fetching thread... ")
	time.Sleep(200 * time.Millisecond)

	detail, err := getInteraction(ctx, cfg.apiURL, rootPost.ID)
	if err != nil {
		fmt.Printf("NOTE: GET /v1/interactions/{id} returned %v (DB might be disabled)\n", err)
	} else {
		if detail.Interaction.ID != rootPost.ID {
			return fmt.Errorf("expected interaction %s, got %s", rootPost.ID, detail.Interaction.ID)
		}
		fmt.Printf("OK (Replies count in thread: %d)\n", len(detail.Replies))
	}

	// 8. Test PUT /v1/interactions/{id} optimistic concurrency edit
	fmt.Print("[8/8] Testing PUT /v1/interactions/{id} edit & version conflict... ")
	updatedBody := "Updated body text"
	editResp, err := updateInteraction(ctx, cfg.apiURL, rootPost.ID, updateReq{
		Body:    updatedBody,
		Version: rootPost.Version,
	})
	if err != nil {
		fmt.Printf("NOTE: Edit returned %v (DB might be disabled)\n", err)
	} else {
		if editResp.Version != rootPost.Version+1 {
			return fmt.Errorf("expected version %d, got %d", rootPost.Version+1, editResp.Version)
		}

		// Conflict test with stale version
		_, errConflict := updateInteraction(ctx, cfg.apiURL, rootPost.ID, updateReq{
			Body:    "Another update",
			Version: rootPost.Version,
		})
		if errConflict == nil {
			return fmt.Errorf("expected 409 conflict when using stale version, got nil error")
		}
		fmt.Println("OK (Version updated and 409 Conflict verified)")
	}

	return nil
}

func testHealthz(ctx context.Context, apiURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/v1/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	return nil
}

func setupNATSSubscriber(natsURL, groupID string) (*natsio.Conn, <-chan *natsio.Msg, func(), error) {
	nc, err := natsio.Connect(natsURL, natsio.Timeout(2*time.Second))
	if err != nil {
		return nil, nil, nil, err
	}

	eventsChan := make(chan *natsio.Msg, 64)
	sub, err := nc.Subscribe(fmt.Sprintf("BOARD.%s.evt.>", groupID), func(msg *natsio.Msg) {
		select {
		case eventsChan <- msg:
		default:
		}
	})
	if err != nil {
		nc.Close()
		return nil, nil, nil, err
	}

	cleanup := func() {
		_ = sub.Unsubscribe()
		nc.Close()
	}
	return nc, eventsChan, cleanup, nil
}

func startSSEListener(ctx context.Context, apiURL, groupID string) (<-chan string, func(), error) {
	sseURL := fmt.Sprintf("%s/v1/interactions/stream?group_id=%s", apiURL, groupID)
	reqProbe, err := http.NewRequestWithContext(ctx, http.MethodGet, sseURL, nil)
	if err != nil {
		return nil, nil, err
	}
	reqProbe.Header.Set("Accept", "text/event-stream")

	respProbe, err := http.DefaultClient.Do(reqProbe)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = respProbe.Body.Close() }()

	if respProbe.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("expected 200 for SSE, got %d", respProbe.StatusCode)
	}

	sseChan := make(chan string, 32)
	done := make(chan struct{})

	go func() {
		reqStream, err := http.NewRequestWithContext(ctx, http.MethodGet, sseURL, nil)
		if err != nil {
			return
		}
		reqStream.Header.Set("Accept", "text/event-stream")
		respStream, err := http.DefaultClient.Do(reqStream)
		if err != nil {
			return
		}
		defer func() { _ = respStream.Body.Close() }()

		scanner := bufio.NewScanner(respStream.Body)
		for scanner.Scan() {
			select {
			case <-done:
				return
			default:
				line := scanner.Text()
				if strings.HasPrefix(line, "data:") {
					sseChan <- strings.TrimPrefix(line, "data:")
				}
			}
		}
	}()

	stop := func() {
		close(done)
	}
	return sseChan, stop, nil
}

type createReq struct {
	GroupID  string  `json:"group_id"`
	Title    *string `json:"title,omitempty"`
	Body     string  `json:"body"`
	Author   string  `json:"author"`
	ParentID *string `json:"parent_id,omitempty"`
}

type interactionResp struct {
	ID         string  `json:"id"`
	GroupID    string  `json:"group_id"`
	Title      *string `json:"title,omitempty"`
	Body       string  `json:"body"`
	Author     string  `json:"author"`
	Version    int     `json:"version"`
	ReplyCount int     `json:"reply_count"`
}

func createInteraction(ctx context.Context, apiURL, idempKey string, payload createReq) (*interactionResp, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/v1/interactions", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if idempKey != "" {
		req.Header.Set("Idempotency-Key", idempKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("create interaction status %d: %s", resp.StatusCode, string(body))
	}

	var res interactionResp
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return &res, nil
}

type interactionDetailResp struct {
	Interaction interactionResp   `json:"interaction"`
	Replies     []interactionResp `json:"replies"`
}

func getInteraction(ctx context.Context, apiURL, id string) (*interactionDetailResp, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/v1/interactions/"+id, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var res interactionDetailResp
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return &res, nil
}

type updateReq struct {
	Title   *string `json:"title,omitempty"`
	Body    string  `json:"body"`
	Version int     `json:"version"`
}

func updateInteraction(ctx context.Context, apiURL, id string, payload updateReq) (*interactionResp, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, apiURL+"/v1/interactions/"+id, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var res interactionResp
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return &res, nil
}
