package app

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

	"github.com/rayo1uo/llm-async-gateway/internal/config"
)

// Run loads configuration, connects to Redis, and serves the selected role until signaled.
// It returns a process exit code.
func Run(role Role, args []string) int {
	cfg, err := config.Load(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		return 2
	}
	logger := newLogger(cfg.LogLevel)
	if err := serve(role, cfg, logger); err != nil {
		logger.Error("process stopped", "err", err, "role", role.String())
		return 1
	}
	return 0
}

func serve(role Role, cfg config.Config, logger *slog.Logger) error {
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

	application := NewRole(cfg, rdb, logger, nil, role)
	application.Start(ctx)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           application.Handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.Addr, "role", role.String(), "upstream", cfg.UpstreamURL, "pool", cfg.Pool)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown requested", "role", role.String())
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

// String renders the role for logs.
func (r Role) String() string {
	switch r {
	case RoleAll:
		return "all"
	case RoleAPI:
		return "api"
	case RoleDispatch:
		return "dispatcher"
	case RoleControl:
		return "controller"
	default:
		return "custom"
	}
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
