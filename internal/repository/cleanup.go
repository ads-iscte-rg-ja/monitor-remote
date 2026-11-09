package repository

import (
	"context"
	"fmt"
	"time"
)

// CleanupDelivered removes at most limit acknowledged records and their queue
// entries together. Pending/quarantined records and AUTOINCREMENT are untouched.
func (r *QueueRepository) CleanupDelivered(ctx context.Context, days, limit int, now time.Time) (int, error) {
	if days < 0 || days > 36500 || limit < 1 || limit > 10000 {
		return 0, fmt.Errorf("invalid delivered-record retention policy")
	}
	if days == 0 {
		return 0, nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,record_type,record_id FROM delivery_status
 WHERE sent_at IS NOT NULL AND quarantine_reason IS NULL AND julianday(sent_at)<julianday(?)
 ORDER BY id LIMIT ?`, now.UTC().AddDate(0, 0, -days).Format(time.RFC3339Nano), limit)
	if err != nil {
		return 0, err
	}
	type delivered struct {
		queueID, recordID int64
		kind              string
	}
	var records []delivered
	for rows.Next() {
		var record delivered
		if err := rows.Scan(&record.queueID, &record.kind, &record.recordID); err != nil {
			rows.Close()
			return 0, err
		}
		records = append(records, record)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, record := range records {
		var table string
		switch record.kind {
		case "machine_stat":
			table = "machine_stats"
		case "error_log":
			table = "error_logs"
		default:
			return 0, fmt.Errorf("invalid delivered record type")
		}
		// Only constant table names above enter SQL; identifiers are never input.
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE id=?", record.recordID); err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM delivery_status WHERE id=? AND sent_at IS NOT NULL AND quarantine_reason IS NULL", record.queueID); err != nil {
			return 0, err
		}
	}
	return len(records), tx.Commit()
}
