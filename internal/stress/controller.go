package stress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// PressureController orchestrates concurrent simulated users generating load against Kozlony.
type PressureController struct {
	cfg        Config
	pool       *InteractionPool
	metrics    *Metrics
	limiter    *rate.Limiter
	httpClient *http.Client
}

// NewPressureController creates a load generator controller.
func NewPressureController(cfg Config, pool *InteractionPool, metrics *Metrics) *PressureController {
	var limiter *rate.Limiter
	if cfg.RPS > 0 {
		burst := int(math.Ceil(cfg.RPS*1.5)) + 1
		limiter = rate.NewLimiter(rate.Limit(cfg.RPS), burst)
	}

	transport := &http.Transport{
		MaxIdleConns:        cfg.Users * 4,
		MaxIdleConnsPerHost: cfg.Users * 4,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	return &PressureController{
		cfg:        cfg,
		pool:       pool,
		metrics:    metrics,
		limiter:    limiter,
		httpClient: client,
	}
}

// Run starts the stress test and blocks until the duration expires or ctx is cancelled.
func (c *PressureController) Run(ctx context.Context, out io.Writer) error {
	runCtx := ctx
	var cancel context.CancelFunc
	if c.cfg.Duration > 0 {
		runCtx, cancel = context.WithTimeout(ctx, c.cfg.Duration)
		defer cancel()
	}

	// Live status line reporter goroutine
	reporterDone := make(chan struct{})
	go func() {
		defer close(reporterDone)
		ticker := time.NewTicker(c.cfg.ReportInterval)
		defer ticker.Stop()

		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				c.metrics.PrintLive(out, c.pool.Size())
			}
		}
	}()

	// Launch worker goroutines
	var wg sync.WaitGroup
	wg.Add(c.cfg.Users)
	for i := 1; i <= c.cfg.Users; i++ {
		workerID := i
		go func() {
			defer wg.Done()
			c.workerLoop(runCtx, workerID)
		}()
	}

	wg.Wait()
	<-reporterDone

	c.metrics.PrintSummary(out, c.pool.Size())
	return nil
}

func (c *PressureController) workerLoop(ctx context.Context, workerID int) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*1000)) //nolint:gosec
	author := fmt.Sprintf("user-%03d", workerID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if c.limiter != nil {
			if err := c.limiter.Wait(ctx); err != nil {
				return
			}
		}

		action := c.selectAction(rng)
		c.executeAction(ctx, rng, author, action)

		// Think time jitter between actions
		if c.cfg.MaxThink > 0 {
			var think time.Duration
			if c.cfg.MaxThink > c.cfg.MinThink {
				delta := c.cfg.MaxThink - c.cfg.MinThink
				think = c.cfg.MinThink + time.Duration(rng.Int63n(int64(delta)))
			} else {
				think = c.cfg.MinThink
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(think):
			}
		}
	}
}

type actionType string

const (
	actionCreateRoot  actionType = "create_root"
	actionCreateReply actionType = "create_reply"
	actionEdit        actionType = "edit"
	actionRead        actionType = "read"
)

const fieldBody = "body"

func (c *PressureController) selectAction(rng *rand.Rand) actionType {
	// If pool is empty, replies and edits cannot succeed; seed root
	if c.pool.Size() == 0 {
		return actionCreateRoot
	}

	total := c.cfg.RootWeight + c.cfg.ReplyWeight + c.cfg.EditWeight + c.cfg.ReadWeight
	roll := rng.Intn(total)

	if roll < c.cfg.RootWeight {
		return actionCreateRoot
	}
	roll -= c.cfg.RootWeight

	if roll < c.cfg.ReplyWeight {
		return actionCreateReply
	}
	roll -= c.cfg.ReplyWeight

	if roll < c.cfg.EditWeight {
		return actionEdit
	}

	return actionRead
}

func (c *PressureController) executeAction(ctx context.Context, rng *rand.Rand, author string, action actionType) {
	switch action {
	case actionCreateRoot:
		c.doCreateRoot(ctx, rng, author)
	case actionCreateReply:
		c.doCreateReply(ctx, rng, author)
	case actionEdit:
		c.doEdit(ctx, rng, author)
	case actionRead:
		c.doRead(ctx, rng)
	}
}

func (c *PressureController) doCreateRoot(ctx context.Context, rng *rand.Rand, author string) {
	payload := map[string]any{
		"group_id": c.cfg.GroupID,
		"author":   author,
		"title":    fmt.Sprintf("Stress Post #%d from %s", rng.Intn(100000), author),
		fieldBody:  fmt.Sprintf("Root discussion content generated at %v with random noise %d", time.Now().Format(time.RFC3339Nano), rng.Int63()),
	}

	c.sendPostInteraction(ctx, "create_root", payload)
}

func (c *PressureController) doCreateReply(ctx context.Context, rng *rand.Rand, author string) {
	parent, ok := c.pool.GetRandomParent()
	if !ok {
		c.doCreateRoot(ctx, rng, author)
		return
	}

	payload := map[string]any{
		"group_id":  c.cfg.GroupID,
		"parent_id": parent.ID.String(),
		"author":    author,
		fieldBody:   fmt.Sprintf("Reply to %s at depth %d by %s (ts: %d)", parent.ID, parent.Depth+1, author, time.Now().UnixNano()),
	}

	c.sendPostInteraction(ctx, "create_reply", payload)
}

func (c *PressureController) sendPostInteraction(ctx context.Context, action string, payload map[string]any) {
	data, _ := json.Marshal(payload)
	reqURL := fmt.Sprintf("%s/v1/interactions", c.cfg.TargetURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(data))
	if err != nil {
		c.metrics.Record(action, 0, 0, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", uuid.New().String())

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	lat := time.Since(start)

	if err != nil {
		c.metrics.Record(action, 0, lat, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	c.metrics.Record(action, resp.StatusCode, lat, nil)

	if resp.StatusCode == http.StatusCreated {
		var created struct {
			ID       uuid.UUID  `json:"id"`
			RootID   *uuid.UUID `json:"root_id"`
			ParentID *uuid.UUID `json:"parent_id"`
			Depth    int        `json:"depth"`
			Version  int        `json:"version"`
			Author   string     `json:"author"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&created); err == nil {
			c.pool.Add(InteractionRef{
				ID:       created.ID,
				RootID:   created.RootID,
				ParentID: created.ParentID,
				Depth:    created.Depth,
				Version:  created.Version,
				Author:   created.Author,
			})
		}
	}
}

func (c *PressureController) doEdit(ctx context.Context, rng *rand.Rand, author string) {
	item, ok := c.pool.GetRandomForEdit()
	if !ok {
		c.doCreateRoot(ctx, rng, author)
		return
	}

	payload := map[string]any{
		fieldBody: fmt.Sprintf("Updated by %s at %v (v%d)", author, time.Now().Format(time.RFC3339Nano), item.Version),
		"version": item.Version,
	}

	data, _ := json.Marshal(payload)
	reqURL := fmt.Sprintf("%s/v1/interactions/%s", c.cfg.TargetURL, item.ID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, bytes.NewReader(data))
	if err != nil {
		c.metrics.Record("edit", 0, 0, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	lat := time.Since(start)

	if err != nil {
		c.metrics.Record("edit", 0, lat, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	c.metrics.Record("edit", resp.StatusCode, lat, nil)

	if resp.StatusCode == http.StatusOK {
		c.pool.UpdateVersion(item.ID, item.Version+1)
	}
}

func (c *PressureController) doRead(ctx context.Context, rng *rand.Rand) {
	var reqURL string
	var action string

	if item, ok := c.pool.GetRandomParent(); ok && rng.Intn(2) == 1 {
		action = "read_interaction"
		reqURL = fmt.Sprintf("%s/v1/interactions/%s", c.cfg.TargetURL, item.ID)
	} else {
		action = "read_feed"
		reqURL = fmt.Sprintf("%s/v1/interactions?group_id=%s&limit=25", c.cfg.TargetURL, c.cfg.GroupID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		c.metrics.Record(action, 0, 0, err)
		return
	}

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	lat := time.Since(start)

	if err != nil {
		c.metrics.Record(action, 0, lat, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	c.metrics.Record(action, resp.StatusCode, lat, nil)
}
