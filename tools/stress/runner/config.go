package runner

import (
	"errors"
	"flag"
	"fmt"
	"time"
)

// MaxAllowedWorkers is the upper limit for concurrent execution worker goroutines to prevent OS socket exhaustion.
const MaxAllowedWorkers = 2000

// Config defines the runtime configuration for the load generator and pressure controller.
type Config struct {
	TargetURL            string
	Users                int           // Population size of simulated users
	Workers              int           // Execution worker pool concurrency
	RPS                  float64       // Target requests per second ceiling (0 for unconstrained)
	MinBody              int           // Minimum interaction body character length (default: 0)
	MaxBody              int           // Maximum interaction body character length (default: 1000)
	Duration             time.Duration // Total test duration (0 for indefinite)
	GroupID              string
	ReportInterval       time.Duration
	RootWeight           int
	ReplyWeight          int
	EditWeight           int
	ReadWeight           int
	WithClients          int           // Number of concurrent real-time SSE stream subscriber clients (default: 0)
	ClientSwitchInterval time.Duration // Interval after which SSE clients switch streams (0 to disable switching)
	ThreadClientRatio    float64       // Ratio of SSE clients assigned to specific threads vs root feed (0.0 to 1.0, default 0.7)
	MinThink             time.Duration // Simulated per-user cooldown before its next request
	MaxThink             time.Duration // Simulated per-user cooldown before its next request
}

// ThinkCeiling reports the highest request rate this configuration can sustain.
//
// Think time is a property of a user, not of a worker: while one user is thinking, the
// worker is free to act as another. So it only limits offered load when there are no
// enough distinct users to keep every worker busy. With a large user population the
// ceiling is set by the workers and the service, not by the cooldown.
func (c Config) ThinkCeiling() float64 {
	avg := (c.MinThink + c.MaxThink) / 2
	if avg <= 0 {
		return 0
	}

	byPopulation := float64(c.Users) / avg.Seconds()
	if byPopulation <= float64(c.Workers) {
		return byPopulation
	}
	// Every worker can stay busy, so cooldown no longer bounds the rate.
	return 0
}

// DefaultConfig returns reasonable default settings for a stress test session.
func DefaultConfig() Config {
	return Config{
		TargetURL:            "http://localhost:8080",
		Users:                1000,
		Workers:              200,
		RPS:                  0,
		MinBody:              0,
		MaxBody:              1000,
		Duration:             0,
		GroupID:              "root",
		ReportInterval:       1 * time.Second,
		RootWeight:           20,
		ReplyWeight:          55,
		EditWeight:           15,
		ReadWeight:           10,
		WithClients:          0,
		ClientSwitchInterval: 10 * time.Second,
		ThreadClientRatio:    0.7,
	}
}

// ParseFlags registers CLI flags and parses command-line arguments into Config.
func ParseFlags(args []string) (Config, error) {
	cfg := DefaultConfig()
	fs := flag.NewFlagSet("stresstest", flag.ContinueOnError)

	fs.StringVar(&cfg.TargetURL, "url", cfg.TargetURL, "Base URL of the Kozlony API")
	fs.IntVar(&cfg.Users, "users", cfg.Users, "Simulated user identity population size (e.g. 1000000)")
	fs.IntVar(&cfg.Workers, "workers", cfg.Workers, "Execution worker pool concurrency (default: 200, max: 2000)")
	fs.Float64Var(&cfg.RPS, "rps", cfg.RPS, "Global max requests per second ceiling (0 for unconstrained)")
	fs.DurationVar(&cfg.MinThink, "min-think", 0, "Simulated per-user cooldown before the next request (models many users posting occasionally rather than a bot hammering)")
	fs.DurationVar(&cfg.MaxThink, "max-think", 0, "Upper bound for -min-think; both zero means continuous pressure")
	fs.IntVar(&cfg.MinBody, "min-body", cfg.MinBody, "Minimum interaction body character length (default: 0)")
	fs.IntVar(&cfg.MaxBody, "max-body", cfg.MaxBody, "Maximum interaction body character length (default: 1000)")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "Total test duration (0 for indefinite until SIGINT)")
	fs.StringVar(&cfg.GroupID, "group", cfg.GroupID, "Target board/group ID")
	fs.DurationVar(&cfg.ReportInterval, "report-interval", cfg.ReportInterval, "Interval between live terminal progress updates")
	fs.IntVar(&cfg.RootWeight, "weight-root", cfg.RootWeight, "Probability weight for creating root posts")
	fs.IntVar(&cfg.ReplyWeight, "weight-reply", cfg.ReplyWeight, "Probability weight for replying to existing interactions")
	fs.IntVar(&cfg.EditWeight, "weight-edit", cfg.EditWeight, "Probability weight for editing existing interactions")
	fs.IntVar(&cfg.ReadWeight, "weight-read", cfg.ReadWeight, "Probability weight for reading feeds and threads")
	fs.IntVar(&cfg.WithClients, "with-clients", cfg.WithClients, "Number of concurrent live SSE stream subscriber clients (default: 0)")
	fs.DurationVar(&cfg.ClientSwitchInterval, "client-switch-interval", cfg.ClientSwitchInterval, "Interval after which SSE clients switch streams (e.g. 10s, 0 to disable)")
	fs.Float64Var(&cfg.ThreadClientRatio, "thread-client-ratio", cfg.ThreadClientRatio, "Ratio of SSE clients assigned to specific threads vs root feed (0.0 to 1.0)")

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}

	if err := cfg.Validate(); err != nil {
		return cfg, err
	}

	return cfg, nil
}

// Validate checks whether the configuration values are sound.
func (c *Config) Validate() error {
	if c.TargetURL == "" {
		return errors.New("target URL cannot be empty")
	}
	if c.Users <= 0 {
		return errors.New("users population must be greater than 0")
	}
	if c.Workers <= 0 {
		return errors.New("workers must be greater than 0")
	}
	if c.Workers > MaxAllowedWorkers {
		return fmt.Errorf("workers (%d) exceeds safe execution pool limit of %d to protect OS resources", c.Workers, MaxAllowedWorkers)
	}
	if c.WithClients < 0 {
		return errors.New("with-clients must be non-negative")
	}
	if c.WithClients > 50000 {
		return errors.New("with-clients exceeds maximum safe client limit of 50000")
	}
	if c.ThreadClientRatio < 0 || c.ThreadClientRatio > 1.0 {
		return errors.New("thread-client-ratio must be between 0.0 and 1.0")
	}
	if c.ClientSwitchInterval < 0 {
		return errors.New("client-switch-interval cannot be negative")
	}
	if c.MinBody < 0 || c.MaxBody < 0 {
		return errors.New("body lengths must be non-negative")
	}
	if c.MinBody > c.MaxBody {
		return fmt.Errorf("min-body (%d) cannot exceed max-body (%d)", c.MinBody, c.MaxBody)
	}
	if c.MaxBody > 65535 {
		return fmt.Errorf("max-body (%d) exceeds API limit of 65535", c.MaxBody)
	}
	if c.MinThink < 0 || c.MaxThink < 0 {
		return errors.New("think times cannot be negative")
	}
	if c.MinThink > 0 && c.MaxThink > 0 && c.MinThink > c.MaxThink {
		return fmt.Errorf("min-think (%v) cannot exceed max-think (%v)", c.MinThink, c.MaxThink)
	}
	if c.RPS < 0 {
		return errors.New("rps must be non-negative")
	}
	totalWeight := c.RootWeight + c.ReplyWeight + c.EditWeight + c.ReadWeight
	if totalWeight <= 0 {
		return errors.New("total action weights must be greater than 0")
	}
	return nil
}
