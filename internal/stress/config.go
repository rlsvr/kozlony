package stress

import (
	"errors"
	"flag"
	"fmt"
	"time"
)

// Config defines the runtime configuration for the load generator and pressure controller.
type Config struct {
	TargetURL      string
	Users          int
	RPS            float64
	MinThink       time.Duration
	MaxThink       time.Duration
	Duration       time.Duration
	GroupID        string
	ReportInterval time.Duration
	RootWeight     int
	ReplyWeight    int
	EditWeight     int
	ReadWeight     int
}

// DefaultConfig returns reasonable default settings for a stress test session.
func DefaultConfig() Config {
	return Config{
		TargetURL:      "http://localhost:8080",
		Users:          20,
		RPS:            0,
		MinThink:       1 * time.Second,
		MaxThink:       3 * time.Second,
		Duration:       30 * time.Second,
		GroupID:        "root",
		ReportInterval: 1 * time.Second,
		RootWeight:     20,
		ReplyWeight:    55,
		EditWeight:     15,
		ReadWeight:     10,
	}
}

// ParseFlags registers CLI flags and parses command-line arguments into Config.
func ParseFlags(args []string) (Config, error) {
	cfg := DefaultConfig()
	fs := flag.NewFlagSet("stresstest", flag.ContinueOnError)

	fs.StringVar(&cfg.TargetURL, "url", cfg.TargetURL, "Base URL of the Kozlony API")
	fs.IntVar(&cfg.Users, "users", cfg.Users, "Number of concurrent simulated users")
	fs.Float64Var(&cfg.RPS, "rps", cfg.RPS, "Global max requests per second ceiling (0 for unconstrained)")
	fs.DurationVar(&cfg.MinThink, "min-think", cfg.MinThink, "Minimum think time between actions per user")
	fs.DurationVar(&cfg.MaxThink, "max-think", cfg.MaxThink, "Maximum think time between actions per user")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "Total test duration (0 for indefinite until SIGINT)")
	fs.StringVar(&cfg.GroupID, "group", cfg.GroupID, "Target board/group ID")
	fs.DurationVar(&cfg.ReportInterval, "report-interval", cfg.ReportInterval, "Interval between live terminal progress updates")
	fs.IntVar(&cfg.RootWeight, "weight-root", cfg.RootWeight, "Probability weight for creating root posts")
	fs.IntVar(&cfg.ReplyWeight, "weight-reply", cfg.ReplyWeight, "Probability weight for replying to existing interactions")
	fs.IntVar(&cfg.EditWeight, "weight-edit", cfg.EditWeight, "Probability weight for editing existing interactions")
	fs.IntVar(&cfg.ReadWeight, "weight-read", cfg.ReadWeight, "Probability weight for reading feeds and threads")

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
		return errors.New("users must be greater than 0")
	}
	if c.MinThink < 0 || c.MaxThink < 0 {
		return errors.New("think times must be non-negative")
	}
	if c.MinThink > c.MaxThink {
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
