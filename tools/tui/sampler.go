package main

import (
	"context"
	"database/sql"
	"os/exec"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// ResourceStats holds CPU and memory metrics for a monitored component.
type ResourceStats struct {
	CPUPercent float64
	RAMMegs    float64
}

// SystemSnapshot captures the resource utilization across the stack.
type SystemSnapshot struct {
	KozlonyPID int
	Kozlony    ResourceStats
	Postgres   ResourceStats
	NATS       ResourceStats

	// Database state
	DBConnected   bool
	DBTotalRows   int64
	DBMaxDepth    int
	DBRootPosts   int64
	DBReplies     int64
	DBReplyCounts int64
}

// Sampler polls system and container resource metrics.
type Sampler struct {
	kozlonyPID int
	dbConnStr  string
	dbPool     *sql.DB
}

// NewSampler creates a new resource sampler.
func NewSampler(kozlonyPID int, dbConnStr string) *Sampler {
	var pool *sql.DB
	if dbConnStr != "" {
		if db, err := sql.Open("pgx", dbConnStr); err == nil {
			pool = db
		}
	}
	return &Sampler{
		kozlonyPID: kozlonyPID,
		dbConnStr:  dbConnStr,
		dbPool:     pool,
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

	// Sample Kozlony Process
	snap.Kozlony.CPUPercent, snap.Kozlony.RAMMegs = s.sampleProcess(ctx)

	// Sample Docker containers for Postgres and NATS
	s.sampleDockerContainers(ctx, &snap)

	// Query DB metrics if connected
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

	return snap
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
