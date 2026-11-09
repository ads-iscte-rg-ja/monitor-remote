package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	// This local package embeds database/schema.sql from the current checkout.
	bufferschema "monitor-remote/database"
)

// Open initializes the current schema. It never migrates or resets old buffers.
func Open(path string) (*sql.DB, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("SQLite path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0750); err != nil {
		return nil, err
	}
	// WAL/SHM are created after the main file has private permissions.
	file, err := os.OpenFile(abs, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	q := u.Query()
	q.Set("mode", "rw") // The private file was created above.
	q.Set("_busy_timeout", "5000")
	q.Set("_foreign_keys", "on")
	q.Set("_journal_mode", "WAL")
	q.Set("_synchronous", "FULL")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	// Reject incompatible development files before applying initialization SQL.
	if err := checkExistingSchema(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("existing buffer is incompatible; use a fresh SQLite path (no migration is performed): %w", err)
	}
	if _, err := db.ExecContext(ctx, bufferschema.SQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize SQLite schema: %w", err)
	}
	return db, nil
}

// Initialization and compatibility checks belong to remote, not the SDK.
// Missing tables are expected on first startup and are created from schema.sql.
func checkExistingSchema(ctx context.Context, db *sql.DB) error {
	for _, column := range []struct {
		table, name string
		primaryKey  bool
	}{
		{"error_logs", "id", true},
		{"machine_stats", "id", true},
		{"delivery_status", "record_id", false},
	} {
		var exists bool
		if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)", column.table).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			continue
		}
		var kind string
		var primaryKey int
		if err := db.QueryRowContext(ctx, "SELECT type,pk FROM pragma_table_info(?) WHERE name=?", column.table, column.name).Scan(&kind, &primaryKey); err != nil {
			return fmt.Errorf("inspect %s.%s: %w", column.table, column.name, err)
		}
		if !strings.EqualFold(kind, "INTEGER") {
			return fmt.Errorf("%s.%s must be INTEGER", column.table, column.name)
		}
		if column.primaryKey && primaryKey != 1 {
			return fmt.Errorf("%s.%s must be the primary key", column.table, column.name)
		}
	}
	var deliveryExists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='delivery_status')").Scan(&deliveryExists); err != nil {
		return err
	}
	if deliveryExists {
		var kind string
		if err := db.QueryRowContext(ctx, "SELECT type FROM pragma_table_info('delivery_status') WHERE name='quarantine_reason'").Scan(&kind); err != nil || !strings.EqualFold(kind, "TEXT") {
			return fmt.Errorf("delivery_status.quarantine_reason must be TEXT")
		}
	}
	return nil
}
