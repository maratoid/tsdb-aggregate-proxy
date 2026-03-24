package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kong"
	"github.com/gofiber/fiber/v2"
	"github.com/maratoid/tsdb-aggregate-proxy/cli"
	"github.com/maratoid/tsdb-aggregate-proxy/metrics"
	"github.com/maratoid/tsdb-aggregate-proxy/ready"
	"github.com/maratoid/tsdb-aggregate-proxy/remoteread"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	kong.Parse(&cli.CLI,
		kong.Name("tsdb-aggregate-proxy"),
		kong.Description("Proxy for Prometheus remote_read endpoints."),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
			Summary: true,
		}))

	logLevel, err := zerolog.ParseLevel(cli.CLI.LogLevel)
	if err != nil {
		log.Panic().Msg(err.Error())
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(logLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("Starting tsdb-aggregate-proxy...")

	app := fiber.New()
	remoteread.Handle(cli.CLI.Path, app)
	ready.Handle(cli.CLI.Healthcheck, app)
	metrics.Handle(cli.CLI.Metrics, app)

	go func() {
		if err := app.Listen(fmt.Sprintf(":%d", cli.CLI.Port)); err != nil {
			log.Panic().Msg(err.Error())
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	log.Info().Msg("Gracefully shutting down...")
	err = app.Shutdown()
	if err != nil {
		log.Warn().Msg(err.Error())
	}

	log.Debug().Msg("Running cleanup tasks...")
	// nothing here yet
	log.Info().Msg("tsdb-aggregate-proxy was shutdown successfully.")
}
