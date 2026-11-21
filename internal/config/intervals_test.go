package config

import (
	"strings"
	"testing"
	"time"
)

func TestCollectionAndDeliveryIntervalBounds(t *testing.T) {
	t.Setenv("CENTRAL_GRPC_URL", "https://central.example")
	t.Setenv("HOST_API_KEY", "test-only-key")
	t.Setenv("COLLECTION_INTERVAL", "30s")
	t.Setenv("STATS_POST_INTERVAL", "30s")
	t.Setenv("ERROR_POST_INTERVAL", "10s")
	t.Setenv("SENT_RETENTION_DAYS", "7")
	t.Setenv("CLEANUP_INTERVAL", "1h")
	t.Setenv("CLEANUP_BATCH_SIZE", "1000")
	for _, setting := range []struct {
		name                           string
		minimum, maximum, defaultValue time.Duration
		read                           func(*Config) time.Duration
	}{
		{"COLLECTION_INTERVAL", 5 * time.Second, time.Hour, 30 * time.Second, func(c *Config) time.Duration { return c.CollectionInterval }},
		{"STATS_POST_INTERVAL", 5 * time.Second, time.Hour, 30 * time.Second, func(c *Config) time.Duration { return c.StatsPostInterval }},
		{"ERROR_POST_INTERVAL", time.Second, 30 * time.Second, 10 * time.Second, func(c *Config) time.Duration { return c.ErrorPostInterval }},
	} {
		t.Run(setting.name, func(t *testing.T) {
			for _, tc := range []struct {
				value string
				want  time.Duration
			}{
				{"", setting.defaultValue},
				{setting.minimum.String(), setting.minimum},
				{setting.maximum.String(), setting.maximum},
				{(setting.minimum - time.Nanosecond).String(), 0},
				{(setting.maximum + time.Nanosecond).String(), 0},
				{"0", 0}, {"-1s", 0}, {"24h", 0}, {"invalid", 0},
			} {
				t.Run(tc.value, func(t *testing.T) {
					t.Setenv(setting.name, tc.value)
					cfg, err := Load()
					if tc.want == 0 {
						if err == nil || !strings.Contains(err.Error(), setting.name) {
							t.Fatal("invalid interval was not rejected with its setting name", err)
						}
					} else if err != nil || setting.read(cfg) != tc.want {
						t.Fatal("valid interval/default was not preserved", cfg, err)
					}
				})
			}
		})
	}
}

func TestCentralGRPCURLIsRequiredAndSecureOutsideLoopback(t *testing.T) {
	t.Setenv("HOST_API_KEY", "test-only-key")
	// The same rule as the gRPC client (ADR-0015): a path is now refused here too.
	for _, value := range []string{"", "not a url", "http://central.example:3334", "central.example:3334", "https://central.example/ingest"} {
		t.Setenv("CENTRAL_GRPC_URL", value)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "CENTRAL_GRPC_URL") {
			t.Fatalf("CENTRAL_GRPC_URL=%q accepted or unclear error: %v", value, err)
		}
	}
	for _, value := range []string{"https://central.example", "http://localhost:3334", "http://127.0.0.1:3334", "http://127.0.0.2:3334"} {
		t.Setenv("CENTRAL_GRPC_URL", value)
		if cfg, err := Load(); err != nil || cfg.CentralURL != value {
			t.Fatalf("CENTRAL_GRPC_URL=%q rejected: %v", value, err)
		}
	}
}
