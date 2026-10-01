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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rlsvr/hirnok/pkg/queue"
	"github.com/rlsvr/hirnok/pkg/worker"
	"golang.org/x/time/rate"
)

// PressureController orchestrates simulated user traffic using a bounded worker pool.
type PressureController struct {
	cfg        Config
	pool       *InteractionPool
	metrics    *Metrics
	limiter    *rate.Limiter
	httpClient *http.Client
}

// NewPressureController creates a load generator controller.
func NewPressureController(cfg Config, pool *InteractionPool, metrics *Metrics) *PressureController {
	poolWorkers := cfg.Workers
	if poolWorkers <= 0 {
		poolWorkers = 200
	}

	var limiter *rate.Limiter
	if cfg.RPS > 0 {
		burst := int(math.Ceil(cfg.RPS*0.2)) + poolWorkers
		if burst < 100 {
			burst = 100
		}
		limiter = rate.NewLimiter(rate.Limit(cfg.RPS), burst)
	}

	transport := &http.Transport{
		MaxIdleConns:        poolWorkers * 2,
		MaxIdleConnsPerHost: poolWorkers * 2,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		DisableCompression:  true,
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

// Run starts the stress test using a fixed worker pool from hirnok and blocks until completion.
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
				snap := c.metrics.PrintLive(out, c.pool.Size())
				_ = SaveRunnerStatus(DefaultStatusFilePath, RunnerStatus{
					Active:    true,
					PID:       os.Getpid(),
					TargetURL: c.cfg.TargetURL,
					Users:     c.cfg.Users,
					TargetRPS: c.cfg.RPS,
					PoolSize:  c.pool.Size(),
					UpdatedAt: time.Now(),
					Snapshot:  snap,
				})
			}
		}
	}()

	numWorkers := c.cfg.Workers
	if numWorkers <= 0 {
		numWorkers = 200
	}

	qCapacity := numWorkers * 4
	if qCapacity < 1000 {
		qCapacity = 1000
	}
	q := queue.New[struct{}](qCapacity)

	var workerSeq atomic.Int64
	wPool := worker.Run(runCtx, numWorkers, q, func(workerCtx context.Context, _ struct{}) {
		id := workerSeq.Add(1)
		rng := rand.New(rand.NewSource(time.Now().UnixNano() + id*1000)) //nolint:gosec
		author := fmt.Sprintf("user-%05d", rng.Intn(c.cfg.Users)+1)
		action := c.selectAction(rng)
		c.executeAction(workerCtx, rng, author, action)

		if c.cfg.MaxThink > 0 {
			var think time.Duration
			if c.cfg.MaxThink > c.cfg.MinThink {
				think = c.cfg.MinThink + time.Duration(rng.Int63n(int64(c.cfg.MaxThink-c.cfg.MinThink)))
			} else {
				think = c.cfg.MinThink
			}
			select {
			case <-workerCtx.Done():
			case <-time.After(think):
			}
		}
	})

	// Dispatcher goroutine: feeds the bounded queue according to RPS or unconstrained
	var dispatchWg sync.WaitGroup
	dispatchWg.Add(1)
	go func() {
		defer dispatchWg.Done()
		defer q.Close()

		if c.limiter != nil {
			for {
				select {
				case <-runCtx.Done():
					return
				default:
				}
				if err := c.limiter.Wait(runCtx); err != nil {
					return
				}
				if err := q.Push(runCtx, struct{}{}); err != nil {
					return
				}
			}
		} else {
			// Unconstrained mode: push whenever queue has capacity
			for {
				select {
				case <-runCtx.Done():
					return
				default:
				}
				if err := q.Push(runCtx, struct{}{}); err != nil {
					return
				}
			}
		}
	}()

	dispatchWg.Wait()
	_ = wPool.Wait(ctx)
	<-reporterDone

	_ = SaveRunnerStatus(DefaultStatusFilePath, RunnerStatus{
		Active:    false,
		PID:       os.Getpid(),
		TargetURL: c.cfg.TargetURL,
		Users:     c.cfg.Users,
		TargetRPS: c.cfg.RPS,
		PoolSize:  c.pool.Size(),
		UpdatedAt: time.Now(),
		Snapshot:  c.metrics.Snapshot(),
	})

	c.metrics.PrintSummary(out, c.pool.Size())
	return nil
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

func (c *PressureController) generateBody(rng *rand.Rand, prefix string) string {
	targetLen := c.cfg.MinBody
	if c.cfg.MaxBody > c.cfg.MinBody {
		targetLen = c.cfg.MinBody + rng.Intn(c.cfg.MaxBody-c.cfg.MinBody+1)
	}
	// The Kozlony API requires non-empty body (min 1 character)
	if targetLen < 1 {
		targetLen = 1
	}

	if len(prefix) >= targetLen {
		return prefix[:targetLen]
	}

	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	b := make([]byte, targetLen)
	copy(b, prefix)
	for i := len(prefix); i < targetLen; i++ {
		b[i] = charset[rng.Intn(len(charset))]
	}
	return string(b)
}

func (c *PressureController) doCreateRoot(ctx context.Context, rng *rand.Rand, author string) {
	prefix := fmt.Sprintf("Root post by %s at %v: ", author, time.Now().Format(time.RFC3339Nano))
	payload := map[string]any{
		"group_id": c.cfg.GroupID,
		"author":   author,
		"title":    fmt.Sprintf("Stress Post #%d from %s", rng.Intn(100000), author),
		fieldBody:  c.generateBody(rng, prefix),
	}

	c.sendPostInteraction(ctx, "create_root", payload)
}

func (c *PressureController) doCreateReply(ctx context.Context, rng *rand.Rand, author string) {
	parent, ok := c.pool.GetRandomParent()
	if !ok {
		c.doCreateRoot(ctx, rng, author)
		return
	}

	prefix := fmt.Sprintf("Reply to %s at depth %d by %s (ts: %d): ", parent.ID, parent.Depth+1, author, time.Now().UnixNano())
	payload := map[string]any{
		"group_id":  c.cfg.GroupID,
		"parent_id": parent.ID.String(),
		"author":    author,
		fieldBody:   c.generateBody(rng, prefix),
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
		if ctx.Err() != nil {
			return
		}
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

	prefix := fmt.Sprintf("Updated by %s at %v (v%d): ", author, time.Now().Format(time.RFC3339Nano), item.Version)
	payload := map[string]any{
		fieldBody: c.generateBody(rng, prefix),
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
		if ctx.Err() != nil {
			return
		}
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
		if ctx.Err() != nil {
			return
		}
		c.metrics.Record(action, 0, lat, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	c.metrics.Record(action, resp.StatusCode, lat, nil)
}
