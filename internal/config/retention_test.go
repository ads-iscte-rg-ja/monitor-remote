package config

import (
	"testing"
	"time"
)

func TestCleanupConfiguration(t *testing.T) {
	t.Setenv("CENTRAL_URL", "https://central.example")
	t.Setenv("HOST_API_KEY", "test-only-key")
	t.Setenv("COLLECTION_INTERVAL", "30s")
	t.Setenv("STATS_POST_INTERVAL", "30s")
	t.Setenv("ERROR_POST_INTERVAL", "10s")
	t.Setenv("SENT_RETENTION_DAYS", "7")
	t.Setenv("CLEANUP_INTERVAL", "1h")
	t.Setenv("CLEANUP_BATCH_SIZE", "1000")
	cfg, err := Load()
	if err != nil || cfg.SentRetentionDays != 7 || cfg.CleanupInterval != time.Hour || cfg.CleanupBatchSize != 1000 {
		t.Fatal(cfg, err)
	}
	t.Setenv("SENT_RETENTION_DAYS", "0")
	if cfg, err := Load(); err != nil || cfg.SentRetentionDays != 0 {
		t.Fatal(cfg, err)
	}
	t.Setenv("SENT_RETENTION_DAYS", "-1")
	if _, err := Load(); err == nil {
		t.Fatal("negative retention accepted")
	}
	t.Setenv("SENT_RETENTION_DAYS", "7")
	t.Setenv("CLEANUP_INTERVAL", "1ns")
	if _, err := Load(); err == nil {
		t.Fatal("unbounded cleanup schedule accepted")
	}
	t.Setenv("CLEANUP_INTERVAL", "1h")
	t.Setenv("CLEANUP_BATCH_SIZE", "0")
	if _, err := Load(); err == nil {
		t.Fatal("invalid cleanup bound accepted")
	}
}
