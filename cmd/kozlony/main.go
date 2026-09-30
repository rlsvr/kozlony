package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"kozlony/internal/api/handler"
	"kozlony/internal/api/server"
	"kozlony/internal/config"
	"kozlony/internal/database"
	"kozlony/internal/drainer"
	"kozlony/internal/messaging"
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
			drainer.New(consumer, repo, cfg).Start(ctx)
		}
	}

	h := handler.New(msgClient, repo)

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	e.Use(middleware.RequestID())
	e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogMethod:   true,
		LogURI:      true,
		LogStatus:   true,
		LogLatency:  true,
		LogError:    true,
		HandleError: true,
		LogValuesFunc: func(_ echo.Context, v middleware.RequestLoggerValues) error {
			if v.Error != nil {
				log.Error().
					Err(v.Error).
					Str("method", v.Method).
					Str("uri", v.URI).
					Int("status", v.Status).
					Dur("latency", v.Latency).
					Msg("http request error")
			} else {
				log.Info().
					Str("method", v.Method).
					Str("uri", v.URI).
					Int("status", v.Status).
					Dur("latency", v.Latency).
					Msg("http request")
			}
			return nil
		},
	}))

	server.RegisterHandlers(e, h)

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
