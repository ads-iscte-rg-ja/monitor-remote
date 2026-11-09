package database_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"monitor-remote/internal/database"
	"monitor-remote/internal/models"
	"monitor-remote/internal/repository"
)

func TestRemoteCreatesAndReopensDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buffer.db")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("test database unexpectedly exists")
	}
	db, err := database.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	entry := &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: "keep across restart"}
	if err := repository.NewQueueRepository(db).SaveErrorLog(context.Background(), entry); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if entry.ID <= 0 {
		t.Fatal("missing numeric ID")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := database.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	pending, err := repository.NewQueueRepository(reopened).ListPendingErrorLogs(context.Background(), 10)
	if err != nil || len(pending) != 1 || pending[0].ErrorLog.ID != entry.ID {
		t.Fatal("initialization lost existing data", err)
	}
}

func TestIncompatibleDevelopmentBufferIsNotMigrated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE error_logs(id TEXT PRIMARY KEY,message TEXT); INSERT INTO error_logs VALUES('old-id','keep me')`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if opened, err := database.Open(path); err == nil {
		opened.Close()
		t.Fatal("incompatible buffer accepted")
	}
	check, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var message string
	if err := check.QueryRow("SELECT message FROM error_logs WHERE id='old-id'").Scan(&message); err != nil || message != "keep me" {
		t.Fatal("existing data was changed", err)
	}
}

func TestSQLiteOptionsSurviveReplacementConnections(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "connections.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Drop the initialization connection to check the replacement's DSN settings.
	db.SetMaxIdleConns(0)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, setting := range []struct {
		name string
		want int
	}{
		{"busy_timeout", 5000}, {"foreign_keys", 1}, {"synchronous", 2},
	} {
		var value int
		if err := conn.QueryRowContext(context.Background(), "PRAGMA "+setting.name).Scan(&value); err != nil {
			t.Fatal(err)
		}
		if value != setting.want {
			t.Fatalf("%s=%d, want %d", setting.name, value, setting.want)
		}
	}
	var mode string
	if err := conn.QueryRowContext(context.Background(), "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal_mode=%s err=%v", mode, err)
	}
}
