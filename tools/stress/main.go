package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rlsvr/kozlony/internal/stress"
)

func main() {
	cfg, err := stress.ParseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("=== Kozlony Stress Test & Workload Generator ===")
	fmt.Printf("Target URL:       %s\n", cfg.TargetURL)
	fmt.Printf("Simulated Users:  %d distinct user accounts\n", cfg.Users)
	fmt.Printf("Worker Pool:      %d concurrent workers (hirnok/pkg/worker)\n", cfg.Workers)
	if cfg.RPS > 0 {
		fmt.Printf("Target RPS:       %.1f req/s\n", cfg.RPS)
	} else {
		fmt.Printf("Target RPS:       unconstrained (maximum throughput)\n")
	}
	if cfg.MinThink > 0 || cfg.MaxThink > 0 {
		fmt.Printf("Think Time:       %v - %v\n", cfg.MinThink, cfg.MaxThink)
	} else {
		fmt.Printf("Think Time:       0s (immediate execution)\n")
	}
	if cfg.Duration > 0 {
		fmt.Printf("Duration:         %v\n", cfg.Duration)
	} else {
		fmt.Printf("Duration:         indefinite (press Ctrl+C to stop)\n")
	}
	fmt.Printf("Body Length:      %d - %d characters\n", cfg.MinBody, cfg.MaxBody)
	fmt.Printf("Group ID:         %s\n", cfg.GroupID)
	fmt.Println("================================================")

	avgThink := (cfg.MinThink + cfg.MaxThink) / 2
	if avgThink > 0 && cfg.RPS > 0 {
		maxAchievableRPS := float64(cfg.Workers) / avgThink.Seconds()
		if cfg.RPS > maxAchievableRPS {
			fmt.Printf("⚠️  NOTE: With %d workers and avg think time %v, throughput is physically capped at ~%.0f req/s.\n", cfg.Workers, avgThink, maxAchievableRPS)
			fmt.Printf("   To achieve %.0f req/s, set -min-think 0ms -max-think 0ms (pacing is controlled by -rps).\n", cfg.RPS)
			fmt.Println("================================================")
		}
	}

	pool := stress.NewInteractionPool(10000)
	metrics := stress.NewMetrics()

	// Seed pool with existing interactions if available
	fmt.Print("Seeding interaction pool from feed... ")
	if err := pool.SeedFromFeed(ctx, cfg.TargetURL, cfg.GroupID); err != nil {
		fmt.Printf("skipped (%v)\n", err)
	} else {
		fmt.Printf("done (%d items loaded)\n", pool.Size())
	}

	controller := stress.NewPressureController(cfg, pool, metrics)

	fmt.Println("Starting workload generator...")
	if err := controller.Run(ctx, os.Stdout); err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "Controller error: %v\n", err)
		os.Exit(1)
	}
}
