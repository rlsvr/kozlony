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

	writeMaxConns := cfg.DatabaseWriteMaxConns
	if writeMaxConns <= 0 {
		writeMaxConns = 30
	}
	readMaxConns := cfg.DatabaseReadMaxConns
	if readMaxConns <= 0 {
		readMaxConns = 20
	}

	writePool, err := database.ConnectWithLimits(ctx, cfg.DatabaseURL, writeMaxConns, 5)
	if err != nil {
		log.Warn().Err(err).Msg("failed to connect to PostgreSQL write pool (continuing without DB)")
	} else {
		defer writePool.Close()
		log.Info().
			Int("write_max_conns", writeMaxConns).
			Msg("connected to PostgreSQL write pool (drainer + counter)")
	}

	readPool, err := database.ConnectWithLimits(ctx, cfg.DatabaseURL, readMaxConns, 5)
	if err != nil {
		log.Warn().Err(err).Msg("failed to connect to PostgreSQL read pool (falling back to write pool)")
		readPool = writePool
	} else {
		defer readPool.Close()
		log.Info().
			Int("read_max_conns", readMaxConns).
			Msg("connected to PostgreSQL read pool (HTTP API)")
	}

	var repo database.Repository
	if writePool != nil {
		repo = database.NewInteractionRepositoryWithPools(writePool, readPool)
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
	var (
		replyCounter *drainer.Counter
		batchDrainer *drainer.Drainer
	)
	if msgClient != nil && repo != nil {
		counterConsumer, err := messaging.NewCounterConsumer(ctx, msgClient, cfg.IngestPartitions)
		if err != nil {
			log.Warn().Err(err).Msg("failed to create JetStream counter consumer")
		} else {
			replyCounter = drainer.NewCounter(counterConsumer, repo, cfg)
			replyCounter.Start(ctx)
		}

		drainerConsumer, err := messaging.NewPartitionedDrainerConsumer(ctx, msgClient, cfg.IngestPartitions)
		if err != nil {
			log.Warn().Err(err).Msg("failed to create JetStream partitioned drainer consumer")
		} else {
			// drainer.StartWithCountPublisher constructs the Drainer and launches its
			// worker loop in a background goroutine; do not also call Run here or the
			// loop runs twice. The drainer publishes recalculated reply counts after
			// each committed batch, keeping reply_count writes out of the insert path.
			batchDrainer = drainer.StartWithCountPublisher(ctx, drainerConsumer, repo, msgClient, msgPublisher, cfg)
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

	// 1. Stop HTTP server first so no new incoming requests generate publisher messages.
	if err := e.Shutdown(shutdownCtx); err != nil {
		log.Fatal().Err(err).Msg("server forced to shutdown")
	}

	// 2. Stop the drainer workers so in-flight InsertBatchWithCounts finishes cleanly before DB closes.
	if batchDrainer != nil {
		batchDrainer.Stop()
	}

	// 3. Stop the counter workers so in-flight ApplyReplyCounts finishes cleanly before DB closes.
	if replyCounter != nil {
		replyCounter.Stop()
	}

	log.Info().Msg("server exited cleanly")
}
