// Package config loads application configuration from environment variables at startup.
package config

import (
	"context"
	"fmt"
	"time"

	"github.com/sethvargo/go-envconfig"
)

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
	NATSAckWait               time.Duration `env:"NATS_ACK_WAIT,default=30s"`
	NATSConcurrentSubscribers int           `env:"NATS_CONCURRENT_SUBSCRIBERS,default=32"`
	NATSStreamRetention       time.Duration `env:"NATS_STREAM_RETENTION,default=336h"`

	// PostgreSQL persistence configuration
	DatabaseURL string `env:"DATABASE_URL,default=postgres://postgres:postgres@localhost:5433/messageboard?sslmode=disable"`

	// In-memory hot cache configuration
	MaxCachedInteractions int           `env:"MAX_CACHED_INTERACTIONS,default=500000"`
	CacheTTL              time.Duration `env:"CACHE_TTL,default=1h"`

	// Micro-batch drainer configuration
	DrainBatchSize     int           `env:"DRAIN_BATCH_SIZE,default=500"`
	DrainFlushInterval time.Duration `env:"DRAIN_FLUSH_INTERVAL,default=50ms"`
}

// Load reads configuration from environment variables and returns a populated Config.
func Load(ctx context.Context) (*Config, error) {
	var c Config
	if err := envconfig.Process(ctx, &c); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return &c, nil
}
