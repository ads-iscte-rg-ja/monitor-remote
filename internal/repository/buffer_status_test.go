package repository_test

import (
	"context"
	"testing"
	"time"

	"monitor-remote/internal/repository"
)

func TestBufferStatusSnapshot(t *testing.T) {
	db, repo := prepareQueue(t)
	ctx := context.Background()
	now := time.Now().UTC()

	empty, err := repo.BufferStatus(ctx, now)
	if err != nil || empty.Pending != 0 || empty.Quarantined != 0 || empty.OldestPendingAgeSeconds != 0 ||
		empty.SizeBytes <= 0 || len(empty.DroppedByKind) != 0 || !empty.ObservedAt.Equal(now) {
		t.Fatalf("empty buffer: %+v (%v)", empty, err)
	}

	ids := burst(t, db, "INFO", 3)
	if _, err := db.Exec("UPDATE delivery_status SET queued_at=? WHERE record_id=?", now.Add(-90*time.Second).Format(time.RFC3339Nano), ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := repo.Quarantine(ctx, deliveryOf(t, db, ids[2]), "invalid record"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.EnforceBufferLimit(ctx, repository.BufferLimits{Soft: 1, Hard: 1 << 30}, now); err != nil {
		t.Fatal(err)
	}
	burst(t, db, "WARN", 2)
	if _, err := db.Exec("UPDATE delivery_status SET queued_at=? WHERE sent_at IS NULL AND quarantine_reason IS NULL", now.Add(-45*time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	status, err := repo.BufferStatus(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	// The limit dropped the two pending INFO; the quarantined one stays.
	if status.Pending != 2 || status.Quarantined != 1 || status.OldestPendingAgeSeconds != 45 ||
		status.DroppedByKind["INFO"] != 2 || status.SizeBytes <= 0 {
		t.Fatalf("status %+v", status)
	}

	if _, err := db.Exec("ALTER TABLE buffer_drops RENAME TO buffer_drops_moved"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BufferStatus(ctx, now); err == nil {
		t.Fatal("unreadable drop counters not reported")
	}
	if _, err := db.Exec("ALTER TABLE buffer_drops_moved RENAME TO buffer_drops"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE delivery_status SET queued_at='not a time' WHERE sent_at IS NULL AND quarantine_reason IS NULL"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BufferStatus(ctx, now); err == nil {
		t.Fatal("malformed queue time not reported")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BufferStatus(ctx, now); err == nil {
		t.Fatal("closed database not reported")
	}
}
