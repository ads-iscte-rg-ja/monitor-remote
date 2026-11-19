package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"monitor-remote/internal/collect"
	"monitor-remote/internal/config"
	"monitor-remote/internal/database"
	"monitor-remote/internal/repository"
	"monitor-remote/internal/requests"
	"monitor-remote/internal/sender"
)

func main() {
	logger := log.New(
		os.Stdout,
		"remote-agent: ",
		log.LstdFlags,
	)

	cfg, err := config.Load()
	if err != nil {
		logger.Fatalf(
			"load configuration: %v",
			err,
		)
	}

	db, err := database.Open(cfg.SQLitePath)
	if err != nil {
		logger.Fatalf(
			"open SQLite database: %v",
			err,
		)
	}
	defer db.Close()

	queueRepository := repository.NewQueueRepository(db)

	machineCollector := collect.NewMachineStatusCollector(
		cfg.DiskPath,
	)

	requestClient, err := requests.NewClient(
		cfg.CentralURL,
		cfg.HostAPIKey,
	)
	if err != nil {
		logger.Fatalf("create central client: %v", err)
	}
	defer func() { _ = requestClient.Close() }()

	deliverySender := sender.New(
		queueRepository,
		requestClient,
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	logger.Printf(
		"agent started; collection interval=%s; stats post interval=%s; error post interval=%s",
		cfg.CollectionInterval,
		cfg.StatsPostInterval,
		cfg.ErrorPostInterval,
	)

	collectAndStore := func() {
		stat, err := machineCollector.Collect(ctx)
		if err != nil {
			logger.Printf(
				"collect machine status: %v",
				err,
			)
			return
		}

		if err := queueRepository.SaveMachineStat(
			ctx,
			stat,
		); err != nil {
			logger.Printf(
				"save machine status: %v",
				err,
			)
			return
		}

		logger.Printf(
			"machine status stored locally: id=%d",
			stat.ID,
		)
	}

	// Recolhe uma medição quando o agente inicia.
	collectAndStore()

	// Independent workers ensure slow HTTP delivery cannot stop collection or
	// prevent the other record category from being delivered.
	var workers sync.WaitGroup
	for _, run := range []func(){
		func() { runPeriodic(ctx, cfg.CollectionInterval, false, collectAndStore) },
		func() {
			deliverySender.RunMachineStats(ctx, cfg.StatsPostInterval, func(err error) { logger.Printf("statistics delivery: %v", err) })
		},
		func() {
			deliverySender.RunErrorLogs(ctx, cfg.ErrorPostInterval, func(err error) { logger.Printf("error delivery: %v", err) })
		},
		func() { runCleanup(ctx, cfg, queueRepository, logger) },
	} {
		workers.Add(1)
		go func() { defer workers.Done(); run() }()
	}
	workers.Wait()
	logger.Println("agent stopped")
}

func runCleanup(ctx context.Context, cfg *config.Config, repo *repository.QueueRepository, logger *log.Logger) {
	if cfg.SentRetentionDays == 0 {
		return
	}
	ticker := time.NewTicker(cfg.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			removed, err := repo.CleanupDelivered(cleanupCtx, cfg.SentRetentionDays, cfg.CleanupBatchSize, time.Now().UTC())
			cancel()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				logger.Printf("delivered-record cleanup: %v", err)
			} else if removed > 0 {
				logger.Printf("delivered-record cleanup: removed=%d", removed)
			}
		}
	}
}

// Wait after each cycle instead of accumulating ticks while a request is slow.
func runPeriodic(ctx context.Context, interval time.Duration, immediate bool, run func()) {
	if immediate && ctx.Err() == nil {
		run()
	}
	for {
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if ctx.Err() != nil {
				return
			}
			run()
		}
	}
}
