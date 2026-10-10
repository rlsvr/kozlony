package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rlsvr/kozlony/tools/stress/runner"
)

func main() {
	cfg, err := runner.ParseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("=== Kozlony Stress Test & Workload Generator ===")
	fmt.Printf("Target URL:       %s\n", cfg.TargetURL)
	fmt.Printf("Simulated Users:  %d distinct user accounts\n", cfg.Users)
	fmt.Printf("Worker Pool:      %d concurrent workers (continuous pressure)\n", cfg.Workers)
	if cfg.WithClients > 0 {
		fmt.Printf("SSE Clients:      %d concurrent live stream subscribers\n", cfg.WithClients)
	}
	if cfg.RPS > 0 {
		fmt.Printf("Target RPS:       %.1f req/s\n", cfg.RPS)
	} else {
		fmt.Printf("Target RPS:       unconstrained (maximum throughput)\n")
	}
	if cfg.MinThink > 0 || cfg.MaxThink > 0 {
		fmt.Printf("Think Time:       %v - %v per user\n", cfg.MinThink, cfg.MaxThink)
		if ceiling := cfg.ThinkCeiling(); ceiling > 0 {
			fmt.Printf("Offered Load:     ~%.0f req/s ceiling from %d workers\n", ceiling, cfg.Workers)
		}
		if cfg.RPS > 0 && cfg.ThinkCeiling() > 0 && cfg.RPS > cfg.ThinkCeiling() {
			fmt.Printf("NOTE: -rps %.0f is unreachable; think time caps offered load at ~%.0f req/s.\n",
				cfg.RPS, cfg.ThinkCeiling())
		}
	}
	if cfg.Duration > 0 {
		fmt.Printf("Duration:         %v\n", cfg.Duration)
	} else {
		fmt.Printf("Duration:         indefinite (press Ctrl+C to stop)\n")
	}
	fmt.Printf("Body Length:      %d - %d characters\n", cfg.MinBody, cfg.MaxBody)
	fmt.Printf("Group ID:         %s\n", cfg.GroupID)
	fmt.Println("================================================")

	pool := runner.NewInteractionPool(10000)
	metrics := runner.NewMetrics()

	// Seed pool with existing interactions if available
	fmt.Print("Seeding interaction pool from feed... ")
	if err := pool.SeedFromFeed(ctx, cfg.TargetURL, cfg.GroupID); err != nil {
		fmt.Printf("skipped (%v)\n", err)
	} else {
		fmt.Printf("done (%d items loaded)\n", pool.Size())
	}

	controller := runner.NewPressureController(cfg, pool, metrics)

	fmt.Println("Starting workload generator...")
	if err := controller.Run(ctx, os.Stdout); err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "Controller error: %v\n", err)
		os.Exit(1)
	}
}
