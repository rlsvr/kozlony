package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"kozlony/internal/api/handler"
	"kozlony/internal/api/server"
	"kozlony/internal/config"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load(ctx)
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	if cfg.DebugMode {
		e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
			LogMethod:   true,
			LogURI:      true,
			LogStatus:   true,
			LogLatency:  true,
			LogError:    true,
			HandleError: true,
			LogValuesFunc: func(_ echo.Context, v middleware.RequestLoggerValues) error {
				if v.Error != nil {
					log.Printf("[HTTP] %s %s %d %s err=%v", v.Method, v.URI, v.Status, v.Latency, v.Error)
				} else {
					log.Printf("[HTTP] %s %s %d %s", v.Method, v.URI, v.Status, v.Latency)
				}
				return nil
			},
		}))
	}

	h := handler.New()
	server.RegisterHandlers(e, h)

	go func() {
		log.Printf("starting %s HTTP server on %s", cfg.AppName, cfg.Addr)
		if err := e.Start(cfg.Addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("shutting down server...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	if err := e.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("server forced to shutdown: %v", err)
	}
	log.Println("server exited cleanly")
}
