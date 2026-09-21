// Command overpass is a concurrent L7 reverse proxy / load balancer.
//
//	Listen on :8080, manage on :9090:
//
//		overpass -config overpass.json
//
//	Flags -listen, -strategy and -admin-listen override the config file.
//	Shutdown is graceful: SIGINT/SIGTERM stops new work, running servers
//	get 10 seconds to drain, then the health loop exits.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"overpass/internal/admin"
	"overpass/internal/balancer"
	"overpass/internal/config"
	"overpass/internal/metrics"
)

func main() {
	configPath := flag.String("config", "overpass.json", "path to JSON config file")
	listen := flag.String("listen", "", "proxy listen addr (overrides config)")
	adminListen := flag.String("admin-listen", "", "admin listen addr (overrides config)")
	strategy := flag.String("strategy", "", "balancing strategy (overrides config)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("cannot start", "err", err)
		os.Exit(1)
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *adminListen != "" {
		cfg.AdminListen = *adminListen
	}
	if *strategy != "" {
		cfg.Strategy = *strategy
	}
	if err := cfg.Validate(); err != nil {
		log.Error("invalid config", "err", err)
		os.Exit(1)
	}

	m := metrics.New()
	pool, err := balancer.NewPool(cfg, m, log)
	if err != nil {
		log.Error("cannot start", "err", err)
		os.Exit(1)
	}
	pool.Start()

	proxySrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           pool,
		ReadHeaderTimeout: 5 * time.Second,
	}
	adminSrv := &http.Server{
		Addr:              cfg.AdminListen,
		Handler:           admin.New(pool, m, log),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		log.Info("proxy listening", "addr", cfg.Listen, "strategy", cfg.Strategy)
		if err := proxySrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	go func() {
		log.Info("admin listening", "addr", cfg.AdminListen)
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		log.Info("signal received, draining")
	case err := <-errCh:
		log.Error("server failed", "err", err)
	}

	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = proxySrv.Shutdown(shutdown)
	_ = adminSrv.Shutdown(shutdown)
	pool.Stop()
	log.Info("shutdown complete")
}
