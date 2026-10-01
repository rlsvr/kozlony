package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/rlsvr/kozlony/internal/stress"
)

func main() {
	var (
		targetURL   = flag.String("url", "http://localhost:8080", "Base URL of the Kozlony API")
		users       = flag.Int("users", 100, "Number of concurrent simulated users")
		rps         = flag.Float64("rps", 1000, "Target requests per second ceiling (0 for unconstrained)")
		minThink    = flag.Duration("min-think", 10*time.Millisecond, "Minimum think time per user")
		maxThink    = flag.Duration("max-think", 60*time.Millisecond, "Maximum think time per user")
		duration    = flag.Duration("duration", 0, "Test duration (0 for indefinite run)")
		groupID     = flag.String("group", "root", "Target board/group ID")
		kozlonyPID  = flag.Int("pid", 0, "PID of Kozlony server (0 for auto-detect)")
		databaseURL = flag.String("db", "", "PostgreSQL connection string (defaults to local docker port 5433)")
		loadGen     = flag.Bool("load", false, "Start embedded load generator (default false to monitor external stress test)")
	)
	flag.Parse()

	// Auto-detect Kozlony PID if not provided
	detectedPID := *kozlonyPID
	if detectedPID == 0 {
		cmdCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		out, err := exec.CommandContext(cmdCtx, "pgrep", "-f", "kozlony").Output()
		cancel()
		if err == nil {
			pids := strings.Fields(string(out))
			if len(pids) > 0 {
				// Pick the latest PID
				last := pids[len(pids)-1]
				detectedPID, _ = strconv.Atoi(last)
			}
		}
	}

	// Database connection string fallback
	dbConn := *databaseURL
	if dbConn == "" {
		dbConn = os.Getenv("DATABASE_URL")
	}
	if dbConn == "" {
		//nolint:gosec // Local default dev credentials
		dbConn = "postgres://postgres:postgres@localhost:5433/messageboard?sslmode=disable"
	}

	cfg := stress.Config{
		TargetURL:      *targetURL,
		Users:          *users,
		RPS:            *rps,
		MinThink:       *minThink,
		MaxThink:       *maxThink,
		Duration:       *duration,
		GroupID:        *groupID,
		ReportInterval: 1 * time.Second,
		RootWeight:     20,
		ReplyWeight:    55,
		EditWeight:     15,
		ReadWeight:     10,
	}

	pool := stress.NewInteractionPool(10000)
	// Seed pool from feed
	_ = pool.SeedFromFeed(context.Background(), cfg.TargetURL, cfg.GroupID)

	metrics := stress.NewMetrics()
	sampler := NewSampler(detectedPID, dbConn)

	model := NewModel(cfg, pool, metrics, sampler, *loadGen)
	prog := tea.NewProgram(model, tea.WithAltScreen())

	if _, err := prog.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error running TUI monitor: %v\n", err)
		os.Exit(1)
	}
}
