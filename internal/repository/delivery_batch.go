package repository

import (
	"context"
	"fmt"
	"time"
)

func (r *QueueRepository) MarkAttempts(ctx context.Context, ids []int64, at time.Time) error {
	return r.updateDeliveryBatch(ctx, ids, at, `UPDATE delivery_status
 SET attempt_count=attempt_count+1,last_attempt_at=?
 WHERE id=? AND sent_at IS NULL AND quarantine_reason IS NULL`)
}

func (r *QueueRepository) MarkSentBatch(ctx context.Context, ids []int64, at time.Time) error {
	return r.updateDeliveryBatch(ctx, ids, at, `UPDATE delivery_status SET sent_at=?
 WHERE id=? AND sent_at IS NULL AND quarantine_reason IS NULL`)
}

// A batch acknowledgement is committed locally in one transaction. On failure,
// all members remain pending and central's source-ID deduplication makes replay safe.
func (r *QueueRepository) updateDeliveryBatch(ctx context.Context, ids []int64, at time.Time, query string) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer statement.Close()
	for _, id := range ids {
		result, err := statement.ExecContext(ctx, at.UTC().Format(time.RFC3339Nano), id)
		if err != nil {
			return fmt.Errorf("update delivery batch: %w", err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("pending delivery %d was not found", id)
		}
	}
	return tx.Commit()
}
