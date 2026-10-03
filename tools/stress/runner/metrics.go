package runner

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rlsvr/kozlony/tools/status"
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

	// SSE streaming counters
	activeSSEClients  atomic.Int64
	totalSSEEvents    atomic.Int64
	sseErrors         atomic.Int64
	intervalSSEEvents atomic.Int64
}

// SetSSEClients configures the active SSE client count for telemetry.
func (m *Metrics) SetSSEClients(n int) {
	m.activeSSEClients.Store(int64(n))
}

// RecordSSEEvent records a single received live SSE event.
func (m *Metrics) RecordSSEEvent() {
	m.totalSSEEvents.Add(1)
	m.intervalSSEEvents.Add(1)
}

// RecordSSEError records an SSE connection or read failure.
func (m *Metrics) RecordSSEError() {
	m.sseErrors.Add(1)
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

// PrintLive outputs a single progress status line similar to runner CLI and returns the snapshot.
func (m *Metrics) PrintLive(w io.Writer, poolSize int) status.Snapshot {
	snap := m.Snapshot()
	mins := int(snap.Elapsed.Minutes())
	secs := int(snap.Elapsed.Seconds()) % 60

	sseClients := m.activeSSEClients.Load()
	if sseClients > 0 {
		_, _ = fmt.Fprintf(w, "[%02d:%02d] rps=%6.1f | ok=%-7d err=%-5d | p50=%-7s p90=%-7s p99=%-7s max=%-7s | pool=%-5d | sse=%d (%6.1f ev/s)\n",
			mins, secs,
			snap.IntervalRPS,
			snap.SuccessRequests, snap.ErrorRequests,
			snap.IntervalP50.Truncate(100*time.Microsecond),
			snap.IntervalP90.Truncate(100*time.Microsecond),
			snap.IntervalP99.Truncate(100*time.Microsecond),
			snap.IntervalMax.Truncate(100*time.Microsecond),
			poolSize,
			sseClients,
			snap.SSEEventRate,
		)
	} else {
		_, _ = fmt.Fprintf(w, "[%02d:%02d] rps=%6.1f | ok=%-7d err=%-5d | p50=%-7s p90=%-7s p99=%-7s max=%-7s | pool=%-5d\n",
			mins, secs,
			snap.IntervalRPS,
			snap.SuccessRequests, snap.ErrorRequests,
			snap.IntervalP50.Truncate(100*time.Microsecond),
			snap.IntervalP90.Truncate(100*time.Microsecond),
			snap.IntervalP99.Truncate(100*time.Microsecond),
			snap.IntervalMax.Truncate(100*time.Microsecond),
			poolSize,
		)
	}
	return snap
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

	if m.activeSSEClients.Load() > 0 || m.totalSSEEvents.Load() > 0 {
		totalSSE := m.totalSSEEvents.Load()
		var sseRate float64
		if totalDuration.Seconds() > 0 {
			sseRate = float64(totalSSE) / totalDuration.Seconds()
		}
		_, _ = fmt.Fprintln(w, "\n--- Real-Time SSE Clients ---")
		_, _ = fmt.Fprintf(w, "  Active Clients:    %d\n", m.activeSSEClients.Load())
		_, _ = fmt.Fprintf(w, "  Events Received:   %d\n", totalSSE)
		_, _ = fmt.Fprintf(w, "  Event Rate:        %.2f events/s\n", sseRate)
		_, _ = fmt.Fprintf(w, "  Connection Errors: %d\n", m.sseErrors.Load())
	}
	_, _ = fmt.Fprintln(w, "=============================================================")
}

func percent(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return (float64(part) / float64(total)) * 100.0
}

// Snapshot captures the current metrics values and resets the interval counters.
func (m *Metrics) Snapshot() status.Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(m.startTime)
	intervalDuration := now.Sub(m.lastReportTime).Seconds()
	if intervalDuration <= 0 {
		intervalDuration = 1.0
	}

	intervalRPS := float64(m.intervalTotal) / intervalDuration
	intP50, intP90, intP99, intMax := LatencyPercentiles(m.intervalLats)
	allP50, allP90, allP99, allMax := LatencyPercentiles(m.allLatencies)

	totalReqs := m.totalRequests.Load()
	okReqs := m.successRequests.Load()
	errReqs := m.errorRequests.Load()

	var overallRPS float64
	if elapsed.Seconds() > 0 {
		overallRPS = float64(totalReqs) / elapsed.Seconds()
	}

	actions := make(map[string]int64, len(m.actionCounts))
	for k, v := range m.actionCounts {
		actions[k] = v
	}
	codes := make(map[int]int64, len(m.statusCodes))
	for k, v := range m.statusCodes {
		codes[k] = v
	}

	sseEv := m.intervalSSEEvents.Swap(0)
	sseRate := float64(sseEv) / intervalDuration

	m.lastReportTime = now
	m.intervalTotal = 0
	m.intervalSuccess = 0
	m.intervalError = 0
	m.intervalLats = m.intervalLats[:0]

	return status.Snapshot{
		StartTime:       m.startTime,
		Elapsed:         elapsed,
		TotalRequests:   totalReqs,
		SuccessRequests: okReqs,
		ErrorRequests:   errReqs,
		IntervalRPS:     intervalRPS,
		OverallRPS:      overallRPS,
		IntervalP50:     intP50,
		IntervalP90:     intP90,
		IntervalP99:     intP99,
		IntervalMax:     intMax,
		AllP50:          allP50,
		AllP90:          allP90,
		AllP99:          allP99,
		AllMax:          allMax,
		ActionCounts:    actions,
		StatusCodes:     codes,
		SSEClients:      m.activeSSEClients.Load(),
		SSEEvents:       m.totalSSEEvents.Load(),
		SSEEventRate:    sseRate,
	}
}

// Reset clears all counters and latency samples in-place, keeping the instance pointer valid.
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.startTime = now
	m.lastReportTime = now

	m.totalRequests.Store(0)
	m.successRequests.Store(0)
	m.errorRequests.Store(0)

	m.statusCodes = make(map[int]int64)
	m.actionCounts = make(map[string]int64)
	m.errorsByType = make(map[string]int64)

	m.allLatencies = m.allLatencies[:0]

	m.intervalTotal = 0
	m.intervalSuccess = 0
	m.intervalError = 0
	m.intervalLats = m.intervalLats[:0]

	m.totalSSEEvents.Store(0)
	m.sseErrors.Store(0)
	m.intervalSSEEvents.Store(0)
}
