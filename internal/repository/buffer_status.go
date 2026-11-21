package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"monitor-remote/internal/models"
)

// BufferStatus takes a snapshot of the buffer for central (ADR-0010). Every
// statement uses an index: the partial pending and quarantine indexes, and
// MIN(queued_at) per record type on the pending index. DeliveryPaused is the
// sender's state and is left for the caller.
func (r *QueueRepository) BufferStatus(ctx context.Context, now time.Time) (models.BufferStatus, error) {
	status := models.BufferStatus{ObservedAt: now.UTC()}
	if err := r.db.QueryRowContext(ctx, `SELECT
 (SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL AND quarantine_reason IS NULL),
 (SELECT COUNT(*) FROM delivery_status WHERE quarantine_reason IS NOT NULL)`).Scan(&status.Pending, &status.Quarantined); err != nil {
		return status, fmt.Errorf("count buffer entries: %w", err)
	}
	var oldest sql.NullString
	if err := r.db.QueryRowContext(ctx, `SELECT MIN(oldest) FROM (
 SELECT MIN(queued_at) AS oldest FROM delivery_status WHERE record_type='machine_stat' AND sent_at IS NULL AND quarantine_reason IS NULL
 UNION ALL
 SELECT MIN(queued_at) FROM delivery_status WHERE record_type='error_log' AND sent_at IS NULL AND quarantine_reason IS NULL)`).Scan(&oldest); err != nil {
		return status, fmt.Errorf("read oldest pending entry: %w", err)
	}
	if oldest.Valid {
		queued, err := parseSQLiteTime(oldest.String)
		if err != nil {
			return status, fmt.Errorf("read oldest pending entry: %w", err)
		}
		if age := now.Sub(queued); age > 0 {
			status.OldestPendingAgeSeconds = int64(age / time.Second)
		}
	}
	var err error
	if status.SizeBytes, err = r.BufferSize(ctx); err != nil {
		return status, err
	}
	if status.DroppedByKind, err = r.DroppedByKind(ctx); err != nil {
		return status, err
	}
	return status, nil
}
