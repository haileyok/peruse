package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/haileyok/peruse/peruse"
	"github.com/urfave/cli/v2"

	"net/http"
	_ "net/http/pprof"

	_ "github.com/joho/godotenv/autoload"
)

func main() {
	app := cli.App{
		Name: "peruse",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "http-addr",
				EnvVars: []string{"PERUSE_HTTP_ADDR"},
			},
			&cli.StringFlag{
				Name:     "clickhouse-addr",
				EnvVars:  []string{"PERUSE_CLICKHOUSE_ADDR"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "clickhouse-database",
				EnvVars:  []string{"PERUSE_CLICKHOUSE_DATABASE"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "clickhouse-user",
				EnvVars:  []string{"PERUSE_CLICKHOUSE_USER"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "clickhouse-pass",
				EnvVars:  []string{"PERUSE_CLICKHOUSE_PASS"},
				Required: true,
			},
			&cli.StringFlag{
				Name:    "pprof-addr",
				EnvVars: []string{"PERUSE_PPROF_ADDR"},
				Value:   ":10390",
			},
			&cli.StringFlag{
				Name:     "feed-owner-did",
				EnvVars:  []string{"PERUSE_FEED_OWNER_DID"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "service-did",
				EnvVars:  []string{"PERUSE_SERVICE_DID"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "service-endpoint",
				EnvVars:  []string{"PERUSE_SERVICE_ENDPOINT"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "chrono-feed-rkey",
				EnvVars:  []string{"PERUSE_CHRONO_FEED_RKEY"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "suggested-follows-rkey",
				EnvVars:  []string{"PERUSE_SUGGESTED_FOLLOWS_RKEY"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "nervana-endpoint",
				EnvVars:  []string{"PERUSE_NERVANA_ENDPOINT"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "nervana-api-key",
				EnvVars:  []string{"PERUSE_NERVANA_API_KEY"},
				Required: true,
			},
			&cli.StringFlag{
				Name:    "relay-host",
				EnvVars: []string{"PERUSE_RELAY_HOST"},
				Value:   "wss://bsky.network",
			},
			&cli.StringFlag{
				Name:     "cursor-file",
				EnvVars:  []string{"PERUSE_CURSOR_FILE"},
				Required: true,
			},
		},
		Action: run,
	}

	app.Run(os.Args)
}

var run = func(cmd *cli.Context) error {
	ctx := cmd.Context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	server, err := peruse.NewServer(peruse.ServerArgs{
		HttpAddr:             cmd.String("http-addr"),
		ClickhouseAddr:       cmd.String("clickhouse-addr"),
		ClickhouseDatabase:   cmd.String("clickhouse-database"),
		ClickhouseUser:       cmd.String("clickhouse-user"),
		ClickhousePass:       cmd.String("clickhouse-pass"),
		Logger:               logger,
		FeedOwnerDid:         cmd.String("feed-owner-did"),
		ServiceDid:           cmd.String("service-did"),
		ServiceEndpoint:      cmd.String("service-endpoint"),
		ChronoFeedRkey:       cmd.String("chrono-feed-rkey"),
		SuggestedFollowsRkey: cmd.String("suggested-follows-rkey"),
		NervanaEndpoint:      cmd.String("nervana-endpoint"),
		NervanaApiKey:        cmd.String("nervana-api-key"),
		RelayHost:            cmd.String("relay-host"),
		CursorFile:           cmd.String("cursor-file"),
	})
	if err != nil {
		logger.Error("error creating server", "error", err)
		return err
	}

	go func() {
		exitSigs := make(chan os.Signal, 1)
		signal.Notify(exitSigs, syscall.SIGINT, syscall.SIGTERM)

		sig := <-exitSigs

		logger.Info("received os exit signal", "signal", sig)
		cancel()
	}()

	go func() {
		if err := http.ListenAndServe(cmd.String("pprof-addr"), nil); err != nil {
			logger.Error("error starting pprof", "error", err)
		}
	}()

	if err := server.Run(ctx); err != nil {
		logger.Error("error running server", "error", err)
	}

	return nil
}
