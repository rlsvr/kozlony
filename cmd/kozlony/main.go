package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/rlsvr/kozlony/internal/api/handler"
	"github.com/rlsvr/kozlony/internal/api/server"
	"github.com/rlsvr/kozlony/internal/cache"
	"github.com/rlsvr/kozlony/internal/config"
	"github.com/rlsvr/kozlony/internal/database"
	"github.com/rlsvr/kozlony/internal/drainer"
	"github.com/rlsvr/kozlony/internal/messaging"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load configuration")
	}

	zerolog.TimeFieldFormat = time.RFC3339Nano
	level, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	if cfg.DebugMode {
		level = zerolog.DebugLevel
	}
	zerolog.SetGlobalLevel(level)

	if cfg.PrettyLogging {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	}

	log.Info().
		Str("app", cfg.AppName).
		Str("addr", cfg.Addr).
		Str("log_level", level.String()).
		Msg("initializing services")

	dbPool, err := database.ConnectWithLimits(ctx, cfg.DatabaseURL, cfg.DatabaseMaxConns, cfg.DatabaseMinConns)
	if err != nil {
		log.Warn().Err(err).Msg("failed to connect to PostgreSQL database (continuing without DB)")
	} else {
		defer dbPool.Close()
		log.Info().
			Int("max_conns", cfg.DatabaseMaxConns).
			Int("min_conns", cfg.DatabaseMinConns).
			Msg("connected to PostgreSQL database pool")
	}

	var repo database.Repository
	if dbPool != nil {
		repo = database.NewInteractionRepository(dbPool)
	}

	msgClient, err := messaging.Init(ctx, cfg)
	if err != nil {
		log.Warn().Err(err).Msg("failed to connect to NATS JetStream (continuing without NATS)")
	} else {
		defer msgClient.Close()
	}

	var pub handler.Publisher
	var msgPublisher *messaging.Publisher
	if msgClient != nil {
		msgPublisher = messaging.NewPublisher(msgClient)
		pub = msgPublisher
	}

	// The counter consumer applies recalculated reply counts in its own small transaction.
	// It is stopped during shutdown before the database pool closes, so an in-flight
	// ApplyReplyCounts can never race a closed pool.
	var replyCounter *drainer.Counter
	if msgClient != nil && repo != nil {
		counterConsumer, err := messaging.NewCounterConsumer(ctx, msgClient)
		if err != nil {
			log.Warn().Err(err).Msg("failed to create JetStream counter consumer")
		} else {
			replyCounter = drainer.NewCounter(counterConsumer, repo, cfg)
			replyCounter.Start(ctx)
		}

		drainerConsumer, err := messaging.NewDrainerConsumer(ctx, msgClient)
		if err != nil {
			log.Warn().Err(err).Msg("failed to create JetStream drainer consumer")
		} else {
			// drainer.StartWithCountPublisher constructs the Drainer and launches its
			// worker loop in a background goroutine; do not also call Run here or the
			// loop runs twice. The drainer publishes recalculated reply counts after
			// each committed batch, keeping reply_count writes out of the insert path.
			drainer.StartWithCountPublisher(ctx, drainerConsumer, repo, msgClient, msgPublisher, cfg)
		}
	}

	interactionCache := cache.NewMemoryCache(cfg)
	h := handler.New(pub, repo, interactionCache)
	e := server.New(h)

	go func() {
		log.Info().Str("addr", cfg.Addr).Msg("starting HTTP server")
		if err := e.Start(cfg.Addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("HTTP server encountered fatal error")
		}
	}()

	<-ctx.Done()

	log.Info().Msg("shutting down HTTP server...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	// Stop the counter workers before the database pool is closed by the deferred
	// dbPool.Close(), so an in-flight ApplyReplyCounts cannot race a closing pool.
	if replyCounter != nil {
		replyCounter.Stop()
	}

	if err := e.Shutdown(shutdownCtx); err != nil {
		log.Fatal().Err(err).Msg("server forced to shutdown")
	}
	log.Info().Msg("server exited cleanly")
}
