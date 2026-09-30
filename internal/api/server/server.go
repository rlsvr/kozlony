package server

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog/log"
)

// New initializes an Echo server instance with recovery, request ID, and zerolog request logging,
// and registers the OpenAPI handlers.
func New(h ServerInterface) *echo.Echo {
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

	RegisterHandlers(e, h)
	return e
}
