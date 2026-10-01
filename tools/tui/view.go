package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

var (
	colorFG        = lipgloss.Color("#E6E6E6")
	colorMuted     = lipgloss.Color("#7F8490")
	colorAccent    = lipgloss.Color("#5DA9FF")
	colorAccentAlt = lipgloss.Color("#C678DD")
	colorOK        = lipgloss.Color("#7BD88F")
	colorWarn      = lipgloss.Color("#FFD166")
	colorError     = lipgloss.Color("#FF6B6B")
	colorBorder    = lipgloss.Color("#3E4451")

	styleTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorAccent).
			Padding(0, 1)

	styleHeader = lipgloss.NewStyle().
			Foreground(colorFG).
			Background(lipgloss.Color("#21252B")).
			Padding(0, 1)

	styleCard = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorBorder).
			Padding(0, 1)

	styleCardTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorAccentAlt)

	styleMuted = lipgloss.NewStyle().
			Foreground(colorMuted)

	styleOK = lipgloss.NewStyle().
		Bold(true).
		Foreground(colorOK)

	styleWarn = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorWarn)

	styleError = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorError)

	stylePillActive = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#1E1E1E")).
			Background(colorOK).
			Padding(0, 1)

	stylePillPaused = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#1E1E1E")).
			Background(colorWarn).
			Padding(0, 1)

	styleFooter = lipgloss.NewStyle().
			Foreground(colorMuted).
			Padding(0, 1)
)

func renderHeader(m Model) string {
	title := styleTitle.Render("⚡ KOZLONY TUI LOAD & STACK MONITOR")
	clock := styleMuted.Render(time.Now().Format("15:04:05"))

	var statusPill string
	switch {
	case m.externalActive:
		statusPill = stylePillActive.Render(fmt.Sprintf("● RUNNER ACTIVE (PID %d)", m.externalRunner.PID))
	case m.embeddedRunning:
		statusPill = stylePillActive.Render("● EMBEDDED LOAD ACTIVE")
	case m.cfg.Duration > 0 && time.Since(m.startTime) >= m.cfg.Duration:
		statusPill = stylePillPaused.Render("✔ COMPLETED")
	default:
		statusPill = stylePillPaused.Render("⏸ STANDBY (PRESS [SPACE] OR RUN STRESS)")
	}

	var info string
	if m.externalActive {
		info = fmt.Sprintf("target: %s | users: %d | rps: %.0f | pool: %d",
			m.externalRunner.TargetURL, m.externalRunner.Users, m.externalRunner.TargetRPS, m.pool.Size())
	} else {
		info = fmt.Sprintf("target: %s | users: %d | rps: %.0f | pool: %d",
			m.cfg.TargetURL, m.cfg.Users, m.cfg.RPS, m.pool.Size())
	}
	infoStyled := styleMuted.Render(info)

	topLine := lipgloss.JoinHorizontal(lipgloss.Center, title, "  ", clock, "  ", statusPill, "  ", infoStyled)
	return styleHeader.Width(m.width).Render(topLine)
}

func renderWorkloadCard(m Model, cardWidth int) string {
	snap := m.metricsSnap

	cardTitleText := "WORKLOAD & THROUGHPUT"
	if m.externalActive {
		cardTitleText += " (EXTERNAL STRESS RUNNER)"
	} else if m.embeddedRunning {
		cardTitleText += " (EMBEDDED WORKLOAD GENERATOR)"
	} else {
		cardTitleText += " (STANDBY - PRESS [SPACE] TO START EMBEDDED LOAD)"
	}
	cardTitle := styleCardTitle.Render(cardTitleText)

	// Throughput & summary line
	rpsColor := styleOK
	if snap.IntervalRPS < m.cfg.RPS*0.8 && m.cfg.RPS > 0 {
		rpsColor = styleWarn
	}

	totalOk := snap.SuccessRequests
	totalReqs := snap.TotalRequests
	var successRate float64
	if totalReqs > 0 {
		successRate = float64(totalOk) / float64(totalReqs) * 100.0
	}

	line1 := fmt.Sprintf("  %-16s %s (overall: %.1f req/s)   %-16s %d (%.1f%% ok)",
		styleMuted.Render("Throughput:"),
		rpsColor.Render(fmt.Sprintf("%6.1f req/s", snap.IntervalRPS)),
		snap.OverallRPS,
		styleMuted.Render("Requests:"),
		totalReqs,
		successRate,
	)

	// Latency line
	latTitle := styleMuted.Render("  Latencies (Window):")
	latLine := fmt.Sprintf("%s P50: %s | P90: %s | P99: %s | Max: %s",
		latTitle,
		formatDuration(snap.IntervalP50),
		formatDuration(snap.IntervalP90),
		formatDuration(snap.IntervalP99),
		formatDuration(snap.IntervalMax),
	)

	// Action Breakdown & Status Codes side by side
	colWidth := (cardWidth - 8) / 2
	if colWidth < 30 {
		colWidth = 30
	}

	// Actions column
	var actLines []string
	actLines = append(actLines, styleMuted.Render("  ACTIONS BREAKDOWN"))
	acts := make([]string, 0, len(snap.ActionCounts))
	for a := range snap.ActionCounts {
		acts = append(acts, a)
	}
	sort.Strings(acts)
	for _, a := range acts {
		cnt := snap.ActionCounts[a]
		pct := 0.0
		if totalReqs > 0 {
			pct = float64(cnt) / float64(totalReqs) * 100.0
		}
		actLines = append(actLines, fmt.Sprintf("    %-18s %7d (%4.1f%%)", a, cnt, pct))
	}
	if len(acts) == 0 {
		actLines = append(actLines, "    (waiting for workload traffic...)")
	}

	// Status Codes column
	var codeLines []string
	codeLines = append(codeLines, styleMuted.Render("  HTTP STATUS RESPONSES"))
	codes := make([]int, 0, len(snap.StatusCodes))
	for c := range snap.StatusCodes {
		codes = append(codes, c)
	}
	sort.Ints(codes)
	for _, c := range codes {
		cnt := snap.StatusCodes[c]
		pct := 0.0
		if totalReqs > 0 {
			pct = float64(cnt) / float64(totalReqs) * 100.0
		}
		var tag lipgloss.Style
		switch {
		case c >= 200 && c < 300:
			tag = styleOK
		case c == 409:
			tag = styleWarn // OCC Conflict
		case c >= 400 && c < 500:
			tag = styleMuted
		default:
			tag = styleError
		}
		codeLines = append(codeLines, fmt.Sprintf("    %s %7d (%4.1f%%)", tag.Render(fmt.Sprintf("HTTP %d", c)), cnt, pct))
	}
	if len(codes) == 0 {
		codeLines = append(codeLines, "    (waiting for responses...)")
	}

	leftCol := strings.Join(actLines, "\n")
	rightCol := strings.Join(codeLines, "\n")
	tables := lipgloss.JoinHorizontal(lipgloss.Top,
		lipgloss.NewStyle().Width(colWidth).Render(leftCol),
		lipgloss.NewStyle().Width(colWidth).Render(rightCol),
	)

	body := lipgloss.JoinVertical(lipgloss.Left,
		cardTitle,
		"",
		line1,
		latLine,
		"",
		tables,
	)

	return styleCard.Width(cardWidth).Render(body)
}

func renderResourceCard(m Model, cardWidth int) string {
	cardTitle := styleCardTitle.Render("SYSTEM RESOURCES (CPU & RAM)")

	kzCPU := m.sysSnap.Kozlony.CPUPercent
	kzRAM := m.sysSnap.Kozlony.RAMMegs

	pgCPU := m.sysSnap.Postgres.CPUPercent
	pgRAM := m.sysSnap.Postgres.RAMMegs

	natsCPU := m.sysSnap.NATS.CPUPercent
	natsRAM := m.sysSnap.NATS.RAMMegs

	barWidth := 20

	rowKz := fmt.Sprintf("  %-30s  CPU: %5.1f%% %s  RAM: %7.1f MB",
		"Kozlony Service (Echo+App)",
		kzCPU,
		renderBar(kzCPU, 100.0, barWidth),
		kzRAM,
	)

	rowPG := fmt.Sprintf("  %-30s  CPU: %5.1f%% %s  RAM: %7.1f MB",
		"PostgreSQL 17 (Batched Ingest)",
		pgCPU,
		renderBar(pgCPU, 100.0, barWidth),
		pgRAM,
	)

	rowNATS := fmt.Sprintf("  %-30s  CPU: %5.1f%% %s  RAM: %7.1f MB",
		"NATS JetStream (File Engine)",
		natsCPU,
		renderBar(natsCPU, 100.0, barWidth),
		natsRAM,
	)

	body := lipgloss.JoinVertical(lipgloss.Left,
		cardTitle,
		"",
		rowKz,
		rowPG,
		rowNATS,
	)

	return styleCard.Width(cardWidth).Render(body)
}

func renderDatabaseCard(m Model, cardWidth int) string {
	cardTitle := styleCardTitle.Render("DATABASE & HIERARCHICAL CONVERSATION STATE")

	dbStatus := styleOK.Render("ONLINE")
	if !m.sysSnap.DBConnected {
		dbStatus = styleMuted.Render("STANDBY (Direct PG query inactive)")
	}

	line1 := fmt.Sprintf("  %-22s %s | %-16s %d items",
		styleMuted.Render("PostgreSQL State:"), dbStatus,
		styleMuted.Render("In-Memory Pool:"), m.pool.Size(),
	)

	line2 := fmt.Sprintf("  %-22s %7d rows | %-16s %7d | %-16s %7d",
		styleMuted.Render("Total Ingested:"), m.sysSnap.DBTotalRows,
		styleMuted.Render("Root Discussions:"), m.sysSnap.DBRootPosts,
		styleMuted.Render("Replies:"), m.sysSnap.DBReplies,
	)

	line3 := fmt.Sprintf("  %-22s %7d levels | %-24s %7d posts",
		styleMuted.Render("Max Tree Depth:"), m.sysSnap.DBMaxDepth,
		styleMuted.Render("Reply Counts Incremented:"), m.sysSnap.DBReplyCounts,
	)

	body := lipgloss.JoinVertical(lipgloss.Left,
		cardTitle,
		"",
		line1,
		line2,
		line3,
	)

	return styleCard.Width(cardWidth).Render(body)
}

func renderFooter(m Model) string {
	elapsed := time.Since(m.startTime).Truncate(time.Second)
	hints := "[q] Quit  [space] Pause/Resume  [+] RPS +50  [-] RPS -50  [c] Clear Metrics"
	status := fmt.Sprintf("Elapsed: %v | %s", elapsed, hints)
	return styleFooter.Width(m.width).Render(status)
}

func renderBar(val, maxVal float64, width int) string {
	if maxVal <= 0 {
		maxVal = 100.0
	}
	frac := val / maxVal
	if frac > 1.0 {
		frac = 1.0
	}
	if frac < 0 {
		frac = 0
	}

	filled := int(frac * float64(width))
	empty := width - filled

	barStyle := styleOK
	if frac > 0.50 {
		barStyle = styleWarn
	}
	if frac > 0.80 {
		barStyle = styleError
	}

	return fmt.Sprintf("[%s%s]",
		barStyle.Render(strings.Repeat("■", filled)),
		styleMuted.Render(strings.Repeat("░", empty)),
	)
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return styleMuted.Render("-")
	}
	if d < time.Millisecond {
		return styleOK.Render(fmt.Sprintf("%dµs", d.Microseconds()))
	}
	if d < 10*time.Millisecond {
		return styleOK.Render(fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000.0))
	}
	if d < 50*time.Millisecond {
		return styleWarn.Render(fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000.0))
	}
	return styleError.Render(fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000.0))
}
