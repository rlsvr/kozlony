package status

import (
	"encoding/json"
	"os"
	"time"
)

// DefaultStatusFilePath is the standard temporary location where stress runners publish live telemetry.
const DefaultStatusFilePath = "/tmp/kozlony-stress-status.json"

// Snapshot holds a point-in-time metrics state suitable for rendering in dashboards and TUIs.
type Snapshot struct {
	StartTime       time.Time        `json:"start_time"`
	Elapsed         time.Duration    `json:"elapsed"`
	TotalRequests   int64            `json:"total_requests"`
	SuccessRequests int64            `json:"success_requests"`
	ErrorRequests   int64            `json:"error_requests"`
	IntervalRPS     float64          `json:"interval_rps"`
	OverallRPS      float64          `json:"overall_rps"`
	IntervalP50     time.Duration    `json:"interval_p50"`
	IntervalP90     time.Duration    `json:"interval_p90"`
	IntervalP99     time.Duration    `json:"interval_p99"`
	IntervalMax     time.Duration    `json:"interval_max"`
	AllP50          time.Duration    `json:"all_p50"`
	AllP90          time.Duration    `json:"all_p90"`
	AllP99          time.Duration    `json:"all_p99"`
	AllMax          time.Duration    `json:"all_max"`
	ActionCounts    map[string]int64 `json:"action_counts"`
	StatusCodes     map[int]int64    `json:"status_codes"`
	SSEClients      int64            `json:"sse_clients,omitempty"`
	SSEEvents       int64            `json:"sse_events,omitempty"`
	SSEEventRate    float64          `json:"sse_event_rate,omitempty"`
}

// RunnerStatus represents the overall live state of an external stress test runner.
type RunnerStatus struct {
	Active    bool      `json:"active"`
	PID       int       `json:"pid"`
	TargetURL string    `json:"target_url"`
	Users     int       `json:"users"`
	TargetRPS float64   `json:"target_rps"`
	PoolSize  int       `json:"pool_size"`
	UpdatedAt time.Time `json:"updated_at"`
	Snapshot  Snapshot  `json:"snapshot"`
}

// SaveRunnerStatus writes the current runner status to disk as atomic JSON.
func SaveRunnerStatus(path string, s RunnerStatus) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmpFile := path + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmpFile, path)
}

// LoadRunnerStatus reads the runner status from disk.
func LoadRunnerStatus(path string) (RunnerStatus, error) {
	var s RunnerStatus
	//nolint:gosec // Status path is a known status file
	data, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return s, err
	}
	return s, nil
}
