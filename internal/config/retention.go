package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func loadInt(name string, fallback, minimum, maximum int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	number, err := strconv.Atoi(value)
	if err != nil || number < minimum || number > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", name, minimum, maximum)
	}
	return number, nil
}

const (
	defaultBufferMaxMB = 512
	maxBufferMB        = 1 << 20 // 1 TiB
)

// loadStorage reads the local buffer policy: retention and cleanup of delivered
// records, and the buffer limits (ADR-0009).
func loadStorage(cfg *Config) error {
	var err error
	if cfg.SentRetentionDays, err = loadInt("SENT_RETENTION_DAYS", 7, 0, 36500); err != nil {
		return err
	}
	if cfg.CleanupInterval, err = loadBoundedDuration("CLEANUP_INTERVAL", time.Hour, time.Minute, 24*time.Hour); err != nil {
		return err
	}
	if cfg.CleanupBatchSize, err = loadInt("CLEANUP_BATCH_SIZE", 1000, 1, 10000); err != nil {
		return err
	}
	cfg.BufferMaxBytes, cfg.BufferHardMaxBytes, cfg.BufferCheckInterval, err = loadBufferLimits()
	return err
}

// loadBufferLimits reads the soft and hard buffer limits in MiB (the hard limit
// defaults to twice the soft one and may not be below it) and the interval.
func loadBufferLimits() (int64, int64, time.Duration, error) {
	soft, err := loadInt("BUFFER_MAX_MB", defaultBufferMaxMB, 1, maxBufferMB)
	if err != nil {
		return 0, 0, 0, err
	}
	hard, err := loadInt("BUFFER_HARD_MAX_MB", 2*soft, 1, 2*maxBufferMB)
	if err != nil {
		return 0, 0, 0, err
	}
	if hard < soft {
		return 0, 0, 0, fmt.Errorf("BUFFER_HARD_MAX_MB must not be below BUFFER_MAX_MB")
	}
	interval, err := loadBoundedDuration("BUFFER_CHECK_INTERVAL", 10*time.Second, time.Second, 5*time.Minute)
	if err != nil {
		return 0, 0, 0, err
	}
	return int64(soft) << 20, int64(hard) << 20, interval, nil
}
