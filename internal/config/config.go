package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
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
}

func Load() (*Config, error) {
	// A ausência de .env não é um erro: em produção poderão
	// ser utilizadas variáveis definidas diretamente no sistema.
	_ = godotenv.Load()

	centralURL := strings.TrimRight(
		strings.TrimSpace(os.Getenv("CENTRAL_URL")),
		"/",
	)
	if centralURL == "" {
		return nil, fmt.Errorf(
			"CENTRAL_URL is required",
		)
	}

	parsedURL, err := url.Parse(centralURL)
	if err != nil ||
		parsedURL.Scheme == "" ||
		parsedURL.Host == "" {
		return nil, fmt.Errorf(
			"CENTRAL_URL must be a valid URL",
		)
	}

	if parsedURL.Scheme != "https" &&
		!(parsedURL.Scheme == "http" &&
			isLocalHost(parsedURL.Hostname())) {
		return nil, fmt.Errorf(
			"CENTRAL_URL must use HTTPS outside localhost",
		)
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
	retentionDays, err := loadInt("SENT_RETENTION_DAYS", 7, 0, 36500)
	if err != nil {
		return nil, err
	}
	cleanupInterval, err := loadBoundedDuration("CLEANUP_INTERVAL", time.Hour, time.Minute, 24*time.Hour)
	if err != nil {
		return nil, err
	}
	cleanupBatch, err := loadInt("CLEANUP_BATCH_SIZE", 1000, 1, 10000)
	if err != nil {
		return nil, err
	}

	return &Config{
		CentralURL:         centralURL,
		HostAPIKey:         apiKey,
		SQLitePath:         DatabasePath(),
		CollectionInterval: collectionInterval,
		StatsPostInterval:  statsPostInterval,
		ErrorPostInterval:  errorPostInterval,
		DiskPath:           loadDiskPath(),
		SentRetentionDays:  retentionDays,
		CleanupInterval:    cleanupInterval,
		CleanupBatchSize:   cleanupBatch,
	}, nil
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

func isLocalHost(host string) bool {
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
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
