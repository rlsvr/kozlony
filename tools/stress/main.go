package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	fmt.Printf("Concurrent Users: %d\n", cfg.Users)
	if cfg.RPS > 0 {
		fmt.Printf("Max RPS Ceiling:  %.1f req/s\n", cfg.RPS)
	} else {
		fmt.Printf("Max RPS Ceiling:  unconstrained\n")
	}
	fmt.Printf("Think Time:       %v - %v\n", cfg.MinThink, cfg.MaxThink)
	if cfg.Duration > 0 {
		fmt.Printf("Duration:         %v\n", cfg.Duration)
	} else {
		fmt.Printf("Duration:         indefinite (press Ctrl+C to stop)\n")
	}
	fmt.Printf("Group ID:         %s\n", cfg.GroupID)

	avgThink := (cfg.MinThink + cfg.MaxThink) / 2
	if cfg.RPS > 0 && avgThink > 0 {
		cycleTime := avgThink + time.Millisecond
		maxPossibleRPS := float64(cfg.Users) / cycleTime.Seconds()
		if maxPossibleRPS < cfg.RPS*0.85 {
			fmt.Println("------------------------------------------------")
			fmt.Printf("⚠️  NOTE: With %d users and %v avg think time, client max theoretical throughput is ~%.0f req/s.\n",
				cfg.Users, avgThink, maxPossibleRPS)
			suggestedUsers := int(cfg.RPS/500) + 50
			if suggestedUsers < 100 {
				suggestedUsers = 100
			}
			fmt.Printf("   To achieve requested %.0f req/s, pass: -min-think 0s -max-think 0s -users %d\n",
				cfg.RPS, suggestedUsers)
		}
	}
	fmt.Println("================================================")

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
