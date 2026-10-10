// Package config loads application configuration from environment variables at startup.
package config

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sethvargo/go-envconfig"
)

// loadDotEnv reads key-value pairs from .env if present and sets them in os.Environ if not already set.
func loadDotEnv() {
	f, err := os.Open(".env")
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		val = strings.Trim(val, `"'`)
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, val)
		}
	}
}

// Config holds all application configuration values populated from environment variables.
type Config struct {
	AppName         string        `env:"APP_NAME,default=kozlony"`
	Addr            string        `env:"ADDR,default=:8080"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT,default=10s"`
	DebugMode       bool          `env:"DEBUG_MODE,default=false"`
	LogLevel        string        `env:"LOG_LEVEL,default=info"`
	PrettyLogging   bool          `env:"PRETTY_LOGGING,default=true"`

	// NATS JetStream configuration
	NATSURL                   string        `env:"NATS_URL,default=nats://localhost:4222"`
	NATSStreamName            string        `env:"NATS_STREAM_NAME,default=BOARD"`
	NATSConsumerName          string        `env:"NATS_CONSUMER_NAME,default=board-drainer"`
	NATSCounterConsumerName   string        `env:"NATS_COUNTER_CONSUMER_NAME,default=board-counters"`
	NATSAckWait               time.Duration `env:"NATS_ACK_WAIT,default=30s"`
	NATSConcurrentSubscribers int           `env:"NATS_CONCURRENT_SUBSCRIBERS,default=32"`
	NATSStreamRetention       time.Duration `env:"NATS_STREAM_RETENTION,default=336h"`

	// PostgreSQL persistence configuration
	DatabaseURL      string `env:"DATABASE_URL,default=postgres://postgres:postgres@localhost:5433/messageboard?sslmode=disable"`
	DatabaseMaxConns int    `env:"DATABASE_MAX_CONNS,default=50"`
	DatabaseMinConns int    `env:"DATABASE_MIN_CONNS,default=10"`

	// In-memory hot cache configuration
	MaxCachedInteractions int           `env:"MAX_CACHED_INTERACTIONS,default=500000"`
	CacheTTL              time.Duration `env:"CACHE_TTL,default=1h"`

	// Micro-batch drainer configuration
	DrainBatchSize     int           `env:"DRAIN_BATCH_SIZE,default=500"`
	DrainFlushInterval time.Duration `env:"DRAIN_FLUSH_INTERVAL,default=50ms"`
}

// Load reads configuration from environment variables and returns a populated Config.
func Load(ctx context.Context) (*Config, error) {
	loadDotEnv()
	var c Config
	if err := envconfig.Process(ctx, &c); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return &c, nil
}
