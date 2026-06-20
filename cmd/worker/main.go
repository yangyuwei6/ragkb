package main

import (
	"context"
	"flag"
	"fmt"
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

	worker, err := bootstrap.NewWorker(cfg, logger)
	if err != nil {
		logger.Fatal("bootstrap worker failed", zap.Error(err))
	}
	defer worker.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		if err := worker.Run(ctx); err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		logger.Info("shutting down worker...", zap.String("signal", sig.String()))
		cancel()
	case err := <-errCh:
		if err != nil {
			logger.Fatal("worker stopped unexpectedly", zap.Error(err))
		}
		return
	}

	select {
	case err := <-errCh:
		if err != nil {
			logger.Error("worker stopped with error", zap.Error(err))
		}
	case <-time.After(10 * time.Second):
		logger.Warn("worker shutdown timeout")
	}

	logger.Info("worker stopped")
}
