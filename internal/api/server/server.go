package server

import (
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog/log"
)

// Option allows configuring optional Echo server settings.
type Option func(*echo.Echo)

// WithDebug enables pprof diagnostic endpoints on the HTTP server.
func WithDebug(enabled bool) Option {
	return func(e *echo.Echo) {
		if enabled {
			e.GET("/debug/pprof/*", echo.WrapHandler(http.HandlerFunc(pprof.Index)))
			e.GET("/debug/pprof/cmdline", echo.WrapHandler(http.HandlerFunc(pprof.Cmdline)))
			e.GET("/debug/pprof/profile", echo.WrapHandler(http.HandlerFunc(pprof.Profile)))
			e.GET("/debug/pprof/symbol", echo.WrapHandler(http.HandlerFunc(pprof.Symbol)))
			e.GET("/debug/pprof/trace", echo.WrapHandler(http.HandlerFunc(pprof.Trace)))
		}
	}
}

// New initializes an Echo server instance with recovery, request ID, timeouts, body limit,
// and zerolog request logging, and registers OpenAPI handlers.
func New(h ServerInterface, opts ...Option) *echo.Echo {
	e := echo.New()
	e.HideBanner = true

	e.Server.ReadHeaderTimeout = 5 * time.Second
	e.Server.ReadTimeout = 15 * time.Second
	e.Server.IdleTimeout = 120 * time.Second

	e.Use(middleware.Recover())
	e.Use(middleware.RequestID())
	e.Use(middleware.BodyLimit("128K"))
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

	for _, opt := range opts {
		opt(e)
	}

	RegisterHandlers(e, h)
	return e
}
