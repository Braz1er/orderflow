package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"orderflow/internal/config"
	"orderflow/internal/httpapi"
	"orderflow/internal/messaging"
	"orderflow/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if len(os.Args) != 2 {
		log.Error("usage: orderflow api|worker|migrate|healthcheck")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1], log); err != nil {
		log.Error("orderflow stopped", "mode", os.Args[1], "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, mode string, log *slog.Logger) error {
	cfg, err := config.Load(mode)
	if err != nil {
		return err
	}
	switch mode {
	case "healthcheck":
		return healthcheck(ctx, cfg.HTTPAddr)
	case "migrate", "api", "worker":
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("configure PostgreSQL: %w", err)
	}
	defer pool.Close()
	db := store.New(pool)

	switch mode {
	case "migrate":
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := db.Migrate(ctx); err != nil {
			return err
		}
		log.Info("migrations applied")
	case "api":
		cache := redis.NewClient(&redis.Options{
			Addr: cfg.RedisAddr, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
			ContextTimeoutEnabled: true,
		})
		defer cache.Close()
		return serve(ctx, cfg.HTTPAddr, httpapi.New(db, cache, log, cfg.Token, cfg.RateLimit), log)
	case "worker":
		return messaging.Run(ctx, db, cfg, log)
	}
	return nil
}

func serve(ctx context.Context, addr string, handler http.Handler, log *slog.Logger) error {
	server := &http.Server{
		Addr: addr, Handler: handler, ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 12 * time.Second, IdleTimeout: 60 * time.Second,
	}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	log.Info("HTTP API started", "address", addr)
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			return err
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

func healthcheck(ctx context.Context, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(host, port)+"/readyz", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("readiness returned %s", resp.Status)
	}
	return nil
}
