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

	"github.com/rlsvr/kozlony/tools/status"
)

func main() {
	var (
		statusFile  = flag.String("status", status.DefaultStatusFilePath, "Path to stress runner live status JSON file")
		targetURL   = flag.String("url", "http://localhost:8080", "Base URL of the Kozlony API for display")
		kozlonyPID  = flag.Int("pid", 0, "PID of Kozlony server (0 for auto-detect)")
		databaseURL = flag.String("db", "", "PostgreSQL connection string (defaults to local docker port 5433)")
		natsMonURL  = flag.String("nats-mon", "http://localhost:8223", "NATS monitoring HTTP URL (defaults to http://localhost:8223)")
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

	sampler := NewSampler(detectedPID, dbConn, *natsMonURL)
	model := NewModel(*statusFile, *targetURL, sampler)
	prog := tea.NewProgram(model, tea.WithAltScreen())

	if _, err := prog.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error running TUI monitor: %v\n", err)
		os.Exit(1)
	}
}
