package stress

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects real-time performance and error telemetry during stress testing.
type Metrics struct {
	mu sync.Mutex

	startTime time.Time

	// Cumulative counters
	totalRequests   atomic.Int64
	successRequests atomic.Int64
	errorRequests   atomic.Int64

	statusCodes  map[int]int64
	actionCounts map[string]int64
	errorsByType map[string]int64

	// Latency samples
	allLatencies []time.Duration

	// Interval counters for real-time reporting
	lastReportTime  time.Time
	intervalTotal   int64
	intervalSuccess int64
	intervalError   int64
	intervalLats    []time.Duration
}

// NewMetrics initializes a new telemetry collector.
func NewMetrics() *Metrics {
	now := time.Now()
	return &Metrics{
		startTime:      now,
		lastReportTime: now,
		statusCodes:    make(map[int]int64),
		actionCounts:   make(map[string]int64),
		errorsByType:   make(map[string]int64),
		allLatencies:   make([]time.Duration, 0, 10000),
		intervalLats:   make([]time.Duration, 0, 1000),
	}
}

// Record captures the result and latency of a single HTTP request.
func (m *Metrics) Record(action string, statusCode int, duration time.Duration, err error) {
	m.totalRequests.Add(1)

	isSuccess := err == nil && statusCode >= 200 && statusCode < 400
	if isSuccess {
		m.successRequests.Add(1)
	} else {
		m.errorRequests.Add(1)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.actionCounts[action]++
	if statusCode > 0 {
		m.statusCodes[statusCode]++
	}
	if err != nil {
		m.errorsByType[err.Error()]++
	}

	// Reservoir/capped sampling for allLatencies (cap at 100k)
	if len(m.allLatencies) < 100000 {
		m.allLatencies = append(m.allLatencies, duration)
	}

	// Interval tracking
	m.intervalTotal++
	if isSuccess {
		m.intervalSuccess++
	} else {
		m.intervalError++
	}
	if len(m.intervalLats) < 20000 {
		m.intervalLats = append(m.intervalLats, duration)
	}
}

// LatencyPercentiles computes p50, p90, p99, and max from a list of durations.
func LatencyPercentiles(lats []time.Duration) (time.Duration, time.Duration, time.Duration, time.Duration) {
	if len(lats) == 0 {
		return 0, 0, 0, 0
	}
	sorted := make([]time.Duration, len(lats))
	copy(sorted, lats)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	p50 := sorted[int(float64(len(sorted))*0.50)]
	p90 := sorted[int(float64(len(sorted))*0.90)]
	idx99 := int(float64(len(sorted)) * 0.99)
	if idx99 >= len(sorted) {
		idx99 = len(sorted) - 1
	}
	p99 := sorted[idx99]
	maxLat := sorted[len(sorted)-1]
	return p50, p90, p99, maxLat
}

// PrintLive outputs a single progress status line similar to runner CLI.
func (m *Metrics) PrintLive(w io.Writer, poolSize int) {
	m.mu.Lock()
	now := time.Now()
	elapsedTotal := now.Sub(m.startTime).Truncate(time.Second)
	intervalDuration := now.Sub(m.lastReportTime).Seconds()
	if intervalDuration <= 0 {
		intervalDuration = 1.0
	}

	intervalRPS := float64(m.intervalTotal) / intervalDuration
	p50, p90, p99, maxLat := LatencyPercentiles(m.intervalLats)

	totalOk := m.successRequests.Load()
	totalErr := m.errorRequests.Load()

	// Reset interval
	m.lastReportTime = now
	m.intervalTotal = 0
	m.intervalSuccess = 0
	m.intervalError = 0
	m.intervalLats = m.intervalLats[:0]
	m.mu.Unlock()

	mins := int(elapsedTotal.Minutes())
	secs := int(elapsedTotal.Seconds()) % 60

	_, _ = fmt.Fprintf(w, "[%02d:%02d] rps=%6.1f | ok=%-7d err=%-5d | p50=%-7s p90=%-7s p99=%-7s max=%-7s | pool=%-5d\n",
		mins, secs,
		intervalRPS,
		totalOk, totalErr,
		p50.Truncate(100*time.Microsecond),
		p90.Truncate(100*time.Microsecond),
		p99.Truncate(100*time.Microsecond),
		maxLat.Truncate(100*time.Microsecond),
		poolSize,
	)
}

// PrintSummary outputs a detailed final report after the test run finishes.
func (m *Metrics) PrintSummary(w io.Writer, poolSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	totalDuration := time.Since(m.startTime)
	totalReqs := m.totalRequests.Load()
	okReqs := m.successRequests.Load()
	errReqs := m.errorRequests.Load()

	var overallRPS float64
	if totalDuration.Seconds() > 0 {
		overallRPS = float64(totalReqs) / totalDuration.Seconds()
	}

	p50, p90, p99, maxLat := LatencyPercentiles(m.allLatencies)

	_, _ = fmt.Fprintln(w, "\n==================== STRESS TEST SUMMARY ====================")
	_, _ = fmt.Fprintf(w, "Duration:          %v\n", totalDuration.Truncate(time.Millisecond))
	_, _ = fmt.Fprintf(w, "Total Requests:    %d\n", totalReqs)
	_, _ = fmt.Fprintf(w, "Successful (2xx):  %d (%.1f%%)\n", okReqs, percent(okReqs, totalReqs))
	_, _ = fmt.Fprintf(w, "Failed:            %d (%.1f%%)\n", errReqs, percent(errReqs, totalReqs))
	_, _ = fmt.Fprintf(w, "Throughput:        %.2f req/s\n", overallRPS)
	_, _ = fmt.Fprintf(w, "Interaction Pool:  %d items cached\n", poolSize)

	_, _ = fmt.Fprintln(w, "\n--- Latencies ---")
	_, _ = fmt.Fprintf(w, "  P50:             %v\n", p50.Truncate(100*time.Microsecond))
	_, _ = fmt.Fprintf(w, "  P90:             %v\n", p90.Truncate(100*time.Microsecond))
	_, _ = fmt.Fprintf(w, "  P99:             %v\n", p99.Truncate(100*time.Microsecond))
	_, _ = fmt.Fprintf(w, "  Max:             %v\n", maxLat.Truncate(100*time.Microsecond))

	_, _ = fmt.Fprintln(w, "\n--- Actions Breakdown ---")
	actions := make([]string, 0, len(m.actionCounts))
	for a := range m.actionCounts {
		actions = append(actions, a)
	}
	sort.Strings(actions)
	for _, a := range actions {
		count := m.actionCounts[a]
		_, _ = fmt.Fprintf(w, "  %-16s %8d (%.1f%%)\n", a, count, percent(count, totalReqs))
	}

	_, _ = fmt.Fprintln(w, "\n--- HTTP Status Codes ---")
	codes := make([]int, 0, len(m.statusCodes))
	for c := range m.statusCodes {
		codes = append(codes, c)
	}
	sort.Ints(codes)
	for _, c := range codes {
		count := m.statusCodes[c]
		_, _ = fmt.Fprintf(w, "  HTTP %d:         %8d (%.1f%%)\n", c, count, percent(count, totalReqs))
	}

	if len(m.errorsByType) > 0 {
		_, _ = fmt.Fprintln(w, "\n--- Errors ---")
		for errStr, count := range m.errorsByType {
			_, _ = fmt.Fprintf(w, "  [%d] %s\n", count, errStr)
		}
	}
	_, _ = fmt.Fprintln(w, "=============================================================")
}

func percent(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return (float64(part) / float64(total)) * 100.0
}
