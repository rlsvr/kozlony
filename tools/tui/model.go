package main

import (
	"context"
	"io"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/rlsvr/kozlony/internal/stress"
)

type tickMsg time.Time

// Model is the Bubble Tea model managing TUI state, telemetry, and background load.
type Model struct {
	cfg         stress.Config
	pool        *stress.InteractionPool
	metrics     *stress.Metrics
	metricsSnap stress.Snapshot
	sampler     *Sampler
	sysSnap     SystemSnapshot

	startTime       time.Time
	embeddedRunning bool
	externalActive  bool
	externalRunner  stress.RunnerStatus

	width  int
	height int

	runCtx    context.Context
	runCancel context.CancelFunc
}

// NewModel creates an initial TUI model.
func NewModel(cfg stress.Config, pool *stress.InteractionPool, metrics *stress.Metrics, sampler *Sampler, startEmbedded bool) Model {
	ctx, cancel := context.WithCancel(context.Background())
	m := Model{
		cfg:             cfg,
		pool:            pool,
		metrics:         metrics,
		metricsSnap:     metrics.Snapshot(),
		sampler:         sampler,
		startTime:       time.Now(),
		embeddedRunning: startEmbedded,
		width:           100,
		height:          35,
		runCtx:          ctx,
		runCancel:       cancel,
	}

	// Check if an external runner is already active at startup
	extStatus, err := stress.LoadRunnerStatus(stress.DefaultStatusFilePath)
	if err == nil && extStatus.Active && time.Since(extStatus.UpdatedAt) < 4*time.Second {
		m.externalActive = true
		m.externalRunner = extStatus
		m.metricsSnap = extStatus.Snapshot
	} else if startEmbedded {
		m.startWorkload()
	}

	return m
}

func (m *Model) startWorkload() {
	controller := stress.NewPressureController(m.cfg, m.pool, m.metrics)
	m.embeddedRunning = true
	go func() {
		_ = controller.Run(m.runCtx, io.Discard)
	}()
}

// Init sets up the initial 1-second tick loop.
func (Model) Init() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// Update processes Bubble Tea events, hotkeys, and tick notifications.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			if m.runCancel != nil {
				m.runCancel()
			}
			if m.sampler != nil {
				m.sampler.Close()
			}
			return m, tea.Quit

		case " ":
			// Toggle embedded load generation if no external runner is active
			if !m.externalActive {
				if m.embeddedRunning {
					m.embeddedRunning = false
					if m.runCancel != nil {
						m.runCancel()
					}
				} else {
					m.runCtx, m.runCancel = context.WithCancel(context.Background())
					m.startWorkload()
				}
			}

		case "+", "=":
			m.cfg.RPS += 50
			if m.embeddedRunning {
				if m.runCancel != nil {
					m.runCancel()
				}
				m.runCtx, m.runCancel = context.WithCancel(context.Background())
				m.startWorkload()
			}

		case "-", "_":
			if m.cfg.RPS > 50 {
				m.cfg.RPS -= 50
				if m.embeddedRunning {
					if m.runCancel != nil {
						m.runCancel()
					}
					m.runCtx, m.runCancel = context.WithCancel(context.Background())
					m.startWorkload()
				}
			}

		case "c":
			// In-place reset preserves pointer so background workers continue updating
			m.metrics.Reset()
			m.metricsSnap = m.metrics.Snapshot()
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

	case tickMsg:
		if m.cfg.Duration > 0 && time.Since(m.startTime) >= m.cfg.Duration && m.embeddedRunning {
			m.embeddedRunning = false
			if m.runCancel != nil {
				m.runCancel()
			}
		}

		// Check for active external stress test runner
		extStatus, err := stress.LoadRunnerStatus(stress.DefaultStatusFilePath)
		if err == nil && extStatus.Active && time.Since(extStatus.UpdatedAt) < 4*time.Second {
			m.externalActive = true
			m.externalRunner = extStatus
			m.metricsSnap = extStatus.Snapshot
		} else {
			m.externalActive = false
			if m.embeddedRunning {
				m.metricsSnap = m.metrics.Snapshot()
			}
		}

		// Sample system and database state
		if m.sampler != nil {
			m.sysSnap = m.sampler.Sample(context.Background())
		}

		return m, tea.Tick(time.Second, func(t time.Time) tea.Msg {
			return tickMsg(t)
		})
	}

	return m, nil
}

// View constructs the single-pane dashboard layout.
func (m Model) View() string {
	cardWidth := m.width - 4
	if cardWidth < 80 {
		cardWidth = 80
	}

	header := renderHeader(m)
	workloadCard := renderWorkloadCard(m, cardWidth)
	resourceCard := renderResourceCard(m, cardWidth)
	dbCard := renderDatabaseCard(m, cardWidth)
	footer := renderFooter(m)

	content := lipgloss.JoinVertical(lipgloss.Left,
		header,
		"",
		workloadCard,
		"",
		resourceCard,
		"",
		dbCard,
		"",
		footer,
	)

	return lipgloss.NewStyle().Padding(1, 2).Render(content)
}
