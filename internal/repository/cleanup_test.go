package repository_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"monitor-remote/internal/database"
	"monitor-remote/internal/models"
	"monitor-remote/internal/repository"
)

func prepareQueue(t *testing.T) (*sql.DB, *repository.QueueRepository) {
	t.Helper()
	db, err := database.Open(filepath.Join(t.TempDir(), "buffer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, repository.NewQueueRepository(db)
}

func saveError(t *testing.T, db *sql.DB, repo *repository.QueueRepository) (int64, int64) {
	t.Helper()
	e := &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: "failure"}
	if err := repo.SaveErrorLog(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	var deliveryID int64
	if err := db.QueryRow("SELECT id FROM delivery_status WHERE record_type='error_log' AND record_id=?", e.ID).Scan(&deliveryID); err != nil {
		t.Fatal(err)
	}
	return e.ID, deliveryID
}

func TestCleanupPreservesPendingQuarantinedAndSequence(t *testing.T) {
	db, repo := prepareQueue(t)
	ctx := context.Background()
	now := time.Now().UTC()
	oldID, oldQueue := saveError(t, db, repo)
	if err := repo.MarkSent(ctx, oldQueue, now.Add(-10*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	stat := &models.MachineStat{CPUPercent: 25, RAMUsedMB: 1, RAMTotalMB: 2, DiskUsedGB: 1, DiskTotalGB: 2}
	if err := repo.SaveMachineStat(ctx, stat); err != nil {
		t.Fatal(err)
	}
	var statQueue int64
	if err := db.QueryRow("SELECT id FROM delivery_status WHERE record_type='machine_stat' AND record_id=?", stat.ID).Scan(&statQueue); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkSent(ctx, statQueue, now.Add(-10*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	pendingID, _ := saveError(t, db, repo)
	quarantinedID, quarantineQueue := saveError(t, db, repo)
	if err := repo.Quarantine(ctx, quarantineQueue, "invalid record"); err != nil {
		t.Fatal(err)
	}
	recentID, recentQueue := saveError(t, db, repo)
	if err := repo.MarkSent(ctx, recentQueue, now); err != nil {
		t.Fatal(err)
	}
	if n, err := repo.CleanupDelivered(ctx, 0, 1, now); err != nil || n != 0 {
		t.Fatal("disabled cleanup deleted data", n, err)
	}
	if n, err := repo.CleanupDelivered(ctx, 7, 1, now); err != nil || n != 1 {
		t.Fatal("cleanup was not bounded", n, err)
	}
	if n, err := repo.CleanupDelivered(ctx, 7, 100, now); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	for _, id := range []int64{pendingID, quarantinedID, recentID} {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM error_logs WHERE id=?", id).Scan(&count); err != nil || count != 1 {
			t.Fatal("protected payload deleted", id, err)
		}
	}
	var oldCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM error_logs WHERE id=?", oldID).Scan(&oldCount); err != nil || oldCount != 0 {
		t.Fatal("old acknowledged payload retained", err)
	}
	nextID, _ := saveError(t, db, repo)
	if nextID <= recentID {
		t.Fatal("cleanup reset the source sequence")
	}
	if pending, err := repo.ListPendingErrorLogs(ctx, 100); err != nil || len(pending) != 2 {
		t.Fatal("quarantined record returned as pending", err)
	}
}

func TestCleanupPayloadAndQueueDeletionRollBackTogether(t *testing.T) {
	db, repo := prepareQueue(t)
	ctx := context.Background()
	_, queueID := saveError(t, db, repo)
	if err := repo.MarkSent(ctx, queueID, time.Now().Add(-10*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER fail_queue_delete BEFORE DELETE ON delivery_status BEGIN SELECT RAISE(ABORT,'injected cleanup failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CleanupDelivered(ctx, 7, 100, time.Now()); err == nil {
		t.Fatal("expected cleanup rollback")
	}
	for _, table := range []string{"error_logs", "delivery_status"} {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 1 {
			t.Fatal("partial cleanup commit", table, err)
		}
	}
}
