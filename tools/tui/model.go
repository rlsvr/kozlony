package main

import (
	"context"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/rlsvr/kozlony/tools/status"
)

type tickMsg time.Time

// Model is the Bubble Tea model managing pure read-only monitoring state.
type Model struct {
	statusFile   string
	targetURL    string
	sampler      *Sampler
	sysSnap      SystemSnapshot
	metricsSnap  status.Snapshot
	runnerStatus status.RunnerStatus
	runnerActive bool

	viewport  viewport.Model
	ready     bool
	startTime time.Time
	width     int
	height    int
}

// NewModel creates an initial read-only TUI monitor model.
func NewModel(statusFile, targetURL string, sampler *Sampler) Model {
	m := Model{
		statusFile: statusFile,
		targetURL:  targetURL,
		sampler:    sampler,
		startTime:  time.Now(),
		width:      100,
		height:     35,
	}

	// Try reading initial status if file exists
	if s, err := status.LoadRunnerStatus(statusFile); err == nil {
		m.runnerStatus = s
		m.metricsSnap = s.Snapshot
		if s.Active && time.Since(s.UpdatedAt) < 4*time.Second {
			m.runnerActive = true
		}
	}

	return m
}

// Init sets up the initial 1-second tick loop.
func (Model) Init() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// Update processes Bubble Tea events, hotkeys, and tick notifications.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			if m.sampler != nil {
				m.sampler.Close()
			}
			return m, tea.Quit

		case "c":
			// Clear local snapshot display
			m.metricsSnap = status.Snapshot{}
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

		// Header is 1 line, footer is 1 line, leave room for scrollable viewport
		viewportHeight := msg.Height - 2
		if viewportHeight < 5 {
			viewportHeight = 5
		}

		if !m.ready {
			m.viewport = viewport.New(msg.Width, viewportHeight)
			m.ready = true
		} else {
			m.viewport.Width = msg.Width
			m.viewport.Height = viewportHeight
		}

	case tickMsg:
		m.updateRunnerStatus()

		// Sample system and database state
		if m.sampler != nil {
			m.sysSnap = m.sampler.Sample(context.Background())
		}

		return m, tea.Tick(time.Second, func(t time.Time) tea.Msg {
			return tickMsg(t)
		})
	}

	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m *Model) updateRunnerStatus() {
	s, err := status.LoadRunnerStatus(m.statusFile)
	if err != nil {
		m.runnerActive = false
		return
	}

	m.runnerStatus = s
	if s.Active && time.Since(s.UpdatedAt) < 4*time.Second {
		m.runnerActive = true
		m.metricsSnap = s.Snapshot
		return
	}

	m.runnerActive = false
	if s.Snapshot.TotalRequests > 0 && m.metricsSnap.TotalRequests == 0 {
		m.metricsSnap = s.Snapshot
	}
}

// View constructs the single-pane dashboard layout.
func (m Model) View() string {
	cardWidth := m.width - 4
	if cardWidth < 70 {
		cardWidth = 70
	}
	if m.width > 0 && cardWidth > m.width-2 {
		cardWidth = m.width - 2
	}

	header := renderHeader(m)
	workloadCard := renderWorkloadCard(m, cardWidth)
	drainerCard := renderJetStreamCard(m, cardWidth)
	resourceCard := renderResourceCard(m, cardWidth)
	dbCard := renderDatabaseCard(m, cardWidth)
	footer := renderFooter(m)

	cardsBody := lipgloss.JoinVertical(lipgloss.Left,
		workloadCard,
		"",
		drainerCard,
		"",
		resourceCard,
		"",
		dbCard,
	)

	// If viewport is initialized (we received a WindowSizeMsg) and total content exceeds height,
	// use the scrollable viewport so users can see all cards regardless of terminal size.
	if m.ready && m.viewport.Height > 0 {
		m.viewport.SetContent(cardsBody)
		return lipgloss.JoinVertical(lipgloss.Left,
			header,
			m.viewport.View(),
			footer,
		)
	}

	content := lipgloss.JoinVertical(lipgloss.Left,
		header,
		"",
		cardsBody,
		"",
		footer,
	)

	return lipgloss.NewStyle().Padding(0, 1).Render(content)
}
