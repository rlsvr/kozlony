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

	dbPool, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Warn().Err(err).Msg("failed to connect to PostgreSQL database (continuing without DB)")
	} else {
		defer dbPool.Close()
		log.Info().Msg("connected to PostgreSQL database pool")
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

	if msgClient != nil && repo != nil {
		consumer, err := msgClient.CreateDrainerConsumer(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("failed to create JetStream drainer consumer")
		} else {
			d := drainer.New(consumer, repo, msgClient, cfg)
			go func() {
				if err := d.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Error().Err(err).Msg("micro-batch drainer worker exited with error")
				}
			}()
		}
	}

	h := handler.New(msgClient, repo)
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

	if err := e.Shutdown(shutdownCtx); err != nil {
		log.Fatal().Err(err).Msg("server forced to shutdown")
	}
	log.Info().Msg("server exited cleanly")
}
