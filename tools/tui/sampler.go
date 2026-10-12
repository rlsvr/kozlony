package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// ResourceStats holds CPU and memory metrics for a monitored component.
type ResourceStats struct {
	CPUPercent float64
	RAMMegs    float64
}

// ConsumerStats holds metrics for a JetStream pull consumer (drainer or counter).
type ConsumerStats struct {
	Name           string
	Pending        int64
	AckPending     int64
	Redelivered    int64
	WaitingWorkers int
	StreamSeq      uint64
	AckFloorSeq    uint64
}

// JetStreamStats holds stream and consumer statistics sampled from NATS.
type JetStreamStats struct {
	Connected     bool
	StreamName    string
	TotalMessages int64
	TotalBytes    uint64
	LastSeq       uint64
	Drainer       ConsumerStats
	Counter       ConsumerStats
}

// SystemSnapshot captures the resource utilization across the stack.
type SystemSnapshot struct {
	KozlonyPID int
	Kozlony    ResourceStats
	Postgres   ResourceStats
	NATS       ResourceStats

	// JetStream & Drainer pipeline state
	JetStream JetStreamStats

	// Database state
	DBConnected   bool
	DBTotalRows   int64
	DBMaxDepth    int
	DBRootPosts   int64
	DBReplies     int64
	DBReplyCounts int64
}

// Sampler polls system, database, and message broker metrics.
type Sampler struct {
	kozlonyPID int
	dbConnStr  string
	dbPool     *sql.DB
	natsMonURL string
	httpClient *http.Client
}

// NewSampler creates a new resource and pipeline sampler.
func NewSampler(kozlonyPID int, dbConnStr, natsMonURL string) *Sampler {
	var pool *sql.DB
	if dbConnStr != "" {
		if db, err := sql.Open("pgx", dbConnStr); err == nil {
			pool = db
		}
	}
	if natsMonURL == "" {
		natsMonURL = "http://localhost:8223"
	}
	return &Sampler{
		kozlonyPID: kozlonyPID,
		dbConnStr:  dbConnStr,
		dbPool:     pool,
		natsMonURL: natsMonURL,
		httpClient: &http.Client{Timeout: 800 * time.Millisecond},
	}
}

// Close closes any open database pools.
func (s *Sampler) Close() {
	if s.dbPool != nil {
		_ = s.dbPool.Close()
	}
}

// Sample collects a point-in-time resource snapshot.
func (s *Sampler) Sample(ctx context.Context) SystemSnapshot {
	snap := SystemSnapshot{
		KozlonyPID: s.kozlonyPID,
	}

	// 1. Sample JetStream & Drainer metrics (instant HTTP call to NATS monitor)
	s.sampleJetStream(ctx, &snap)

	// 2. Query DB metrics if connected
	if s.dbPool != nil {
		row := s.dbPool.QueryRowContext(ctx, `
			SELECT 
				COUNT(*), 
				COALESCE(MAX(depth), 0), 
				COUNT(*) FILTER (WHERE parent_id IS NULL), 
				COUNT(*) FILTER (WHERE parent_id IS NOT NULL),
				COUNT(*) FILTER (WHERE reply_count > 0)
			FROM interactions
		`)
		if err := row.Scan(&snap.DBTotalRows, &snap.DBMaxDepth, &snap.DBRootPosts, &snap.DBReplies, &snap.DBReplyCounts); err == nil {
			snap.DBConnected = true
		}
	}

	// 3. Sample Kozlony Process
	snap.Kozlony.CPUPercent, snap.Kozlony.RAMMegs = s.sampleProcess(ctx)

	// 4. Sample Docker containers for Postgres and NATS
	s.sampleDockerContainers(ctx, &snap)

	return snap
}

// jszResponse models the subset of the NATS /jsz?consumers=true monitoring endpoint.
type jszResponse struct {
	AccountDetails []struct {
		StreamDetail []struct {
			Name  string `json:"name"`
			State struct {
				Messages int64  `json:"messages"`
				Bytes    uint64 `json:"bytes"`
				LastSeq  uint64 `json:"last_seq"`
			} `json:"state"`
			ConsumerDetail []struct {
				Name      string `json:"name"`
				Delivered struct {
					StreamSeq uint64 `json:"stream_seq"`
				} `json:"delivered"`
				AckFloor struct {
					StreamSeq uint64 `json:"stream_seq"`
				} `json:"ack_floor"`
				NumPending     int64 `json:"num_pending"`
				NumAckPending  int64 `json:"num_ack_pending"`
				NumRedelivered int64 `json:"num_redelivered"`
				NumWaiting     int   `json:"num_waiting"`
			} `json:"consumer_detail"`
		} `json:"stream_detail"`
	} `json:"account_details"`
}

func (s *Sampler) sampleJetStream(ctx context.Context, snap *SystemSnapshot) {
	if s.natsMonURL == "" || s.httpClient == nil {
		return
	}

	reqURL := s.natsMonURL + "/jsz?consumers=true"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return
	}

	var data jszResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return
	}

	for _, acc := range data.AccountDetails {
		for _, st := range acc.StreamDetail {
			if st.Name == "BOARD" {
				snap.JetStream.Connected = true
				snap.JetStream.StreamName = st.Name
				snap.JetStream.TotalMessages = st.State.Messages
				snap.JetStream.TotalBytes = st.State.Bytes
				snap.JetStream.LastSeq = st.State.LastSeq

				for _, c := range st.ConsumerDetail {
					cs := ConsumerStats{
						Name:           c.Name,
						Pending:        c.NumPending,
						AckPending:     c.NumAckPending,
						Redelivered:    c.NumRedelivered,
						WaitingWorkers: c.NumWaiting,
						StreamSeq:      c.Delivered.StreamSeq,
						AckFloorSeq:    c.AckFloor.StreamSeq,
					}
					switch c.Name {
					case "board-drainer":
						snap.JetStream.Drainer = cs
					case "board-counters":
						snap.JetStream.Counter = cs
					}
				}
				return
			}
		}
	}
}

func (s *Sampler) sampleProcess(ctx context.Context) (float64, float64) {
	if s.kozlonyPID <= 0 {
		return 0, 0
	}

	//nolint:gosec // PID is verified integer
	out, err := exec.CommandContext(ctx, "ps", "-p", strconv.Itoa(s.kozlonyPID), "-o", "%cpu,rss", "--no-headers").Output()
	if err != nil {
		return 0, 0
	}

	parts := strings.Fields(string(out))
	if len(parts) < 2 {
		return 0, 0
	}

	cpu, _ := strconv.ParseFloat(parts[0], 64)
	rssKB, _ := strconv.ParseFloat(parts[1], 64)
	return cpu, rssKB / 1024.0
}

func (*Sampler) sampleDockerContainers(ctx context.Context, snap *SystemSnapshot) {
	out, err := exec.CommandContext(ctx, "docker", "stats", "--no-stream", "--format", "{{.Name}},{{.CPUPerc}},{{.MemUsage}}", "kozlony-postgres", "kozlony-nats").Output()
	if err != nil {
		return
	}

	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.Split(line, ",")
		if len(parts) < 3 {
			continue
		}
		name := strings.TrimSpace(parts[0])
		cpu := parseCPUPerc(parts[1])
		mem := parseMemMB(parts[2])

		switch name {
		case "kozlony-postgres":
			snap.Postgres.CPUPercent = cpu
			snap.Postgres.RAMMegs = mem
		case "kozlony-nats":
			snap.NATS.CPUPercent = cpu
			snap.NATS.RAMMegs = mem
		}
	}
}

func parseCPUPerc(val string) float64 {
	clean := strings.TrimSpace(strings.ReplaceAll(val, "%", ""))
	v, _ := strconv.ParseFloat(clean, 64)
	return v
}

func parseMemMB(val string) float64 {
	parts := strings.Split(val, "/")
	if len(parts) == 0 {
		return 0
	}
	raw := strings.TrimSpace(parts[0])
	switch {
	case strings.HasSuffix(raw, "GiB"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(raw, "GiB"), 64)
		return v * 1024.0
	case strings.HasSuffix(raw, "MiB"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(raw, "MiB"), 64)
		return v
	case strings.HasSuffix(raw, "KiB"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(raw, "KiB"), 64)
		return v / 1024.0
	case strings.HasSuffix(raw, "B"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(raw, "B"), 64)
		return v / (1024.0 * 1024.0)
	default:
		return 0
	}
}
