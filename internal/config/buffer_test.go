package config

import (
	"testing"
	"time"
)

func TestBufferLimitConfiguration(t *testing.T) {
	t.Setenv("CENTRAL_GRPC_URL", "https://central.example")
	t.Setenv("HOST_API_KEY", "test-only-key")
	for _, name := range []string{"BUFFER_MAX_MB", "BUFFER_HARD_MAX_MB", "BUFFER_CHECK_INTERVAL"} {
		t.Setenv(name, "")
	}
	cfg, err := Load()
	if err != nil || cfg.BufferMaxBytes != 512<<20 || cfg.BufferHardMaxBytes != 1024<<20 || cfg.BufferCheckInterval != 10*time.Second {
		t.Fatal("defaults", cfg, err)
	}

	t.Setenv("BUFFER_MAX_MB", "64")
	if cfg, err := Load(); err != nil || cfg.BufferHardMaxBytes != 128<<20 {
		t.Fatal("hard limit must default to twice the soft limit", cfg, err)
	}
	t.Setenv("BUFFER_HARD_MAX_MB", "64")
	t.Setenv("BUFFER_CHECK_INTERVAL", "1s")
	if cfg, err := Load(); err != nil || cfg.BufferMaxBytes != 64<<20 || cfg.BufferHardMaxBytes != 64<<20 || cfg.BufferCheckInterval != time.Second {
		t.Fatal("equal limits", cfg, err)
	}

	for _, invalid := range []struct{ name, value string }{
		{"BUFFER_MAX_MB", "0"},
		{"BUFFER_MAX_MB", "2000000"},
		{"BUFFER_MAX_MB", "lots"},
		{"BUFFER_HARD_MAX_MB", "63"},
		{"BUFFER_HARD_MAX_MB", "0"},
		{"BUFFER_CHECK_INTERVAL", "100ms"},
		{"BUFFER_CHECK_INTERVAL", "10m"},
	} {
		t.Run(invalid.name+"="+invalid.value, func(t *testing.T) {
			t.Setenv(invalid.name, invalid.value)
			if _, err := Load(); err == nil {
				t.Fatalf("%s=%s accepted", invalid.name, invalid.value)
			}
		})
	}
}
