package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeEnv(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// unsetForTest removes a variable for this test only (t.Setenv restores it).
func unsetForTest(t *testing.T, name string) {
	t.Helper()
	t.Setenv(name, "")
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
}

// ADR-0011: at start, a key in the process environment wins over .env, and
// Load remembers where it came from so that SIGHUP keeps the same precedence.
func TestLoadRemembersWhereTheKeyCameFrom(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeEnv(t, dir, "CENTRAL_GRPC_URL=https://central.example\nHOST_API_KEY=from-file\n")
	unsetForTest(t, "CENTRAL_GRPC_URL")
	unsetForTest(t, "HOST_API_KEY")
	cfg, err := Load()
	if err != nil || cfg.HostAPIKey != "from-file" || cfg.APIKeyFromEnvironment {
		t.Fatalf("key from .env: %+v %v", cfg, err)
	}
	t.Setenv("HOST_API_KEY", "from-environment")
	cfg, err = Load()
	if err != nil || cfg.HostAPIKey != "from-environment" || !cfg.APIKeyFromEnvironment {
		t.Fatalf("key from the environment: %+v %v", cfg, err)
	}
}

func TestReloadAPIKey(t *testing.T) {
	dir := t.TempDir()
	path := writeEnv(t, dir, "HOST_API_KEY= regenerated-key \n")
	t.Setenv("HOST_API_KEY", "environment-key")
	if key, err := ReloadAPIKey(path, false); err != nil || key != "regenerated-key" {
		t.Fatalf("reload from .env: %q %v", key, err)
	}
	if key, err := ReloadAPIKey(path, true); err != nil || key != "environment-key" {
		t.Fatalf("the process environment must keep precedence: %q %v", key, err)
	}
	if _, err := ReloadAPIKey(filepath.Join(dir, "missing.env"), false); err == nil {
		t.Fatal("a missing .env must be reported")
	}
	if _, err := ReloadAPIKey(writeEnv(t, dir, "OTHER=1\n"), false); err == nil {
		t.Fatal("a .env without the key must be refused")
	}
}
