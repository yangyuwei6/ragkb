package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"ragkb/internal/bootstrap"
	"ragkb/internal/config"
	"ragkb/internal/observability"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load config:", err)
		os.Exit(1)
	}

	logger, err := observability.NewLogger(cfg.Log.Mode, cfg.Log.Level)
	if err != nil {
		fmt.Fprintln(os.Stderr, "init logger:", err)
		os.Exit(1)
	}
	defer logger.Sync()

	app, err := bootstrap.NewAPI(cfg, logger)
	if err != nil {
		logger.Fatal("bootstrap api failed", zap.Error(err))
	}
	defer app.Close()

	go func() {
		logger.Info("http server starting", zap.String("addr", cfg.HTTP.Addr))
		if err := app.Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("http server failed", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("shutting down http server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Server.Shutdown(ctx); err != nil {
		logger.Error("http server shutdown failed", zap.Error(err))
	}
	logger.Info("http server stopped")
}
