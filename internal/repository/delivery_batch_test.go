package repository_test

import (
	"context"
	"testing"
	"time"
)

func TestBatchAcknowledgementRollsBackEveryMember(t *testing.T) {
	db, repo := prepareQueue(t)
	_, first := saveError(t, db, repo)
	_, second := saveError(t, db, repo)
	if _, err := db.Exec(`CREATE TRIGGER reject_second_ack BEFORE UPDATE OF sent_at ON delivery_status
 WHEN NEW.record_id=2 BEGIN SELECT RAISE(ABORT,'injected acknowledgement failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkSentBatch(context.Background(), []int64{first, second}, time.Now()); err == nil {
		t.Fatal("expected batch acknowledgement rollback")
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL AND quarantine_reason IS NULL").Scan(&count); err != nil || count != 2 {
		t.Fatal("only part of the batch was marked delivered", count, err)
	}
	if err := repo.MarkAttempts(context.Background(), []int64{first, 0}, time.Now()); err == nil {
		t.Fatal("expected missing delivery to roll back attempts")
	}
	if err := db.QueryRow("SELECT SUM(attempt_count) FROM delivery_status").Scan(&count); err != nil || count != 0 {
		t.Fatal("batch attempt was partially committed", count, err)
	}
}
