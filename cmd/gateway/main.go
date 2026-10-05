// Command gateway serves the batch and nearline APIs and runs the dispatcher.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/app"
	"github.com/rayo1uo/llm-async-gateway/internal/config"
)

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(2)
	}
	logger := newLogger(cfg.LogLevel)
	if err := run(cfg, logger); err != nil {
		logger.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(cfg config.Config, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer func() { _ = rdb.Close() }()

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := rdb.Ping(pingCtx).Err()
	cancel()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	application := app.New(cfg, rdb, logger, nil)
	application.Start(ctx)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           application.Handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.Addr, "upstream", cfg.UpstreamURL)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-errCh:
		stop()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			application.Wait()
			return err
		}
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown", "err", err)
	}
	stop()
	application.Wait()
	return nil
}

func newLogger(level string) *slog.Logger {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv}))
}
