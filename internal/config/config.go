package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"

	"monitor-remote/internal/validation"
)

const (
	defaultSQLitePath         = "remote-monitor.db"
	defaultCollectionInterval = 30 * time.Second
	defaultStatsPostInterval  = 30 * time.Second
	defaultErrorPostInterval  = 10 * time.Second
)

type Config struct {
	CentralURL         string
	HostAPIKey         string
	DiskPath           string
	SQLitePath         string
	CollectionInterval time.Duration
	StatsPostInterval  time.Duration
	ErrorPostInterval  time.Duration
	SentRetentionDays  int
	CleanupInterval    time.Duration
	CleanupBatchSize   int
	// Buffer limits in bytes and the enforcement interval (ADR-0009).
	BufferMaxBytes      int64
	BufferHardMaxBytes  int64
	BufferCheckInterval time.Duration
	// APIKeyFromEnvironment: HOST_API_KEY came from the process environment,
	// not from .env, so SIGHUP keeps it (ADR-0011).
	APIKeyFromEnvironment bool
}

func Load() (*Config, error) {
	// A key set in the process environment wins over .env, now and on SIGHUP.
	_, keyFromEnvironment := os.LookupEnv("HOST_API_KEY")
	// A ausência de .env não é um erro: em produção poderão
	// ser utilizadas variáveis definidas diretamente no sistema.
	_ = godotenv.Load()

	centralURL := strings.TrimRight(
		strings.TrimSpace(os.Getenv("CENTRAL_GRPC_URL")),
		"/",
	)
	if centralURL == "" {
		return nil, fmt.Errorf(
			"CENTRAL_GRPC_URL is required (gRPC ingestion endpoint, e.g. https://central.example:443)",
		)
	}

	// The same rule as the gRPC client (ADR-0015).
	if _, err := validation.CentralURL(centralURL); err != nil {
		return nil, err
	}

	apiKey := strings.TrimSpace(
		os.Getenv("HOST_API_KEY"),
	)
	if apiKey == "" {
		return nil, fmt.Errorf(
			"HOST_API_KEY is required",
		)
	}

	collectionInterval, err := loadBoundedDuration(
		"COLLECTION_INTERVAL",
		defaultCollectionInterval,
		5*time.Second,
		time.Hour,
	)
	if err != nil {
		return nil, err
	}

	statsPostInterval, err := loadBoundedDuration(
		"STATS_POST_INTERVAL",
		defaultStatsPostInterval,
		5*time.Second,
		time.Hour,
	)
	if err != nil {
		return nil, err
	}

	// Direct SQLite writers do not notify this process. This is the wait between
	// database polling/delivery cycles, not an event-triggered send deadline.
	errorPostInterval, err := loadBoundedDuration(
		"ERROR_POST_INTERVAL",
		defaultErrorPostInterval,
		time.Second,
		30*time.Second,
	)
	if err != nil {
		return nil, err
	}
	cfg := &Config{
		CentralURL:         centralURL,
		HostAPIKey:         apiKey,
		SQLitePath:         DatabasePath(),
		CollectionInterval: collectionInterval,
		StatsPostInterval:  statsPostInterval,
		ErrorPostInterval:  errorPostInterval,
		DiskPath:           loadDiskPath(),

		APIKeyFromEnvironment: keyFromEnvironment,
	}
	if err := loadStorage(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func DatabasePath() string {
	_ = godotenv.Load()

	path := strings.TrimSpace(
		os.Getenv("SQLITE_PATH"),
	)
	if path == "" {
		return defaultSQLitePath
	}

	return path
}

func loadBoundedDuration(name string, defaultValue, minimum, maximum time.Duration) (time.Duration, error) {
	duration := defaultValue
	var err error
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		duration, err = time.ParseDuration(value)
	}
	if err != nil || duration < minimum || duration > maximum {
		return 0, fmt.Errorf("%s must be a duration between %s and %s", name, minimum, maximum)
	}
	return duration, nil
}

func loadDiskPath() string {
	path := strings.TrimSpace(
		os.Getenv("DISK_PATH"),
	)
	if path == "" {
		return "/"
	}

	return path
}

// EnvFile is the configuration file read at start and again on SIGHUP.
const EnvFile = ".env"

// ReloadAPIKey reads HOST_API_KEY again for SIGHUP (ADR-0011), with the same
// precedence as at start: a key from the process environment wins over .env.
// Only the key is reloadable; other settings need a restart.
func ReloadAPIKey(envFile string, fromEnvironment bool) (string, error) {
	if fromEnvironment {
		return strings.TrimSpace(os.Getenv("HOST_API_KEY")), nil
	}
	values, err := godotenv.Read(envFile)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", envFile, err)
	}
	key := strings.TrimSpace(values["HOST_API_KEY"])
	if key == "" {
		return "", fmt.Errorf("HOST_API_KEY is missing from %s", envFile)
	}
	return key, nil
}
