package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"monitor-remote/internal/models"
)

type PendingMachineStat struct {
	DeliveryID    int64
	Stat          models.MachineStat
	InvalidReason string
}

type PendingErrorLog struct {
	DeliveryID    int64
	ErrorLog      models.ErrorLog
	InvalidReason string
}

func (r *QueueRepository) ListPendingMachineStats(
	ctx context.Context,
	limit int,
) ([]PendingMachineStat, error) {
	return r.ListPendingMachineStatsThrough(ctx, limit, 1<<63-1)
}

// DeliveryCutoff freezes the accepted-record boundary of one flush. Concurrent
// application writes receive larger IDs and belong to the next cycle.
func (r *QueueRepository) DeliveryCutoff(ctx context.Context) (int64, error) {
	var cutoff int64
	if err := r.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(id),0) FROM delivery_status").Scan(&cutoff); err != nil {
		return 0, fmt.Errorf("read delivery cutoff: %w", err)
	}
	return cutoff, nil
}

func (r *QueueRepository) ListPendingMachineStatsThrough(ctx context.Context, limit int, cutoff int64) ([]PendingMachineStat, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(
		ctx,
		`SELECT
			ds.id,
			ms.id,
			ms.cpu_percent,
			ms.ram_used_mb,
			ms.ram_total_mb,
			ms.disk_used_gb,
			ms.disk_total_gb,
			ms.collected_at
		 FROM delivery_status ds
		 JOIN machine_stats ms
		   ON ms.id = ds.record_id
		 WHERE ds.record_type = ?
		   AND ds.sent_at IS NULL
		   AND ds.quarantine_reason IS NULL
		   AND ds.id<=?
		 ORDER BY ds.queued_at DESC, ds.id DESC
		 LIMIT ?`,
		models.RecordTypeMachineStat,
		cutoff,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query pending machine stats: %w",
			err,
		)
	}
	defer rows.Close()

	result := make([]PendingMachineStat, 0)

	for rows.Next() {
		var pending PendingMachineStat
		var collectedAt string

		if err := rows.Scan(
			&pending.DeliveryID,
			&pending.Stat.ID,
			&pending.Stat.CPUPercent,
			&pending.Stat.RAMUsedMB,
			&pending.Stat.RAMTotalMB,
			&pending.Stat.DiskUsedGB,
			&pending.Stat.DiskTotalGB,
			&collectedAt,
		); err != nil {
			if pending.DeliveryID <= 0 {
				return nil, fmt.Errorf("scan pending machine stat: %w", err)
			}
			pending.InvalidReason = "stored statistics have invalid field types"
			result = append(result, pending)
			continue
		}

		pending.Stat.CollectedAt, err = parseSQLiteTime(
			collectedAt,
		)
		if err != nil {
			pending.InvalidReason = "stored statistics timestamp is malformed"
		}

		result = append(result, pending)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate pending machine stats: %w",
			err,
		)
	}

	return result, nil
}

func (r *QueueRepository) ListPendingErrorLogs(
	ctx context.Context,
	limit int,
) ([]PendingErrorLog, error) {
	return r.ListPendingErrorLogsThrough(ctx, limit, 1<<63-1)
}

func (r *QueueRepository) ListPendingErrorLogsThrough(ctx context.Context, limit int, cutoff int64) ([]PendingErrorLog, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(
		ctx,
		`SELECT
			ds.id,
			el.id,
			el.app_name,
			el.environment,
			el.level,
			el.message,
			el.stack_trace,
			el.metadata,
			el.created_at
		 FROM delivery_status ds
		 JOIN error_logs el
		   ON el.id = ds.record_id
		 WHERE ds.record_type = ?
		   AND ds.sent_at IS NULL
		   AND ds.quarantine_reason IS NULL
		   AND ds.id<=?
		 ORDER BY ds.queued_at DESC, ds.id DESC
		 LIMIT ?`,
		models.RecordTypeErrorLog,
		cutoff,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query pending error logs: %w",
			err,
		)
	}
	defer rows.Close()

	result := make([]PendingErrorLog, 0)

	for rows.Next() {
		var pending PendingErrorLog
		var stackTrace sql.NullString
		var metadata string
		var createdAt string

		if err := rows.Scan(
			&pending.DeliveryID,
			&pending.ErrorLog.ID,
			&pending.ErrorLog.AppName,
			&pending.ErrorLog.Environment,
			&pending.ErrorLog.Level,
			&pending.ErrorLog.Message,
			&stackTrace,
			&metadata,
			&createdAt,
		); err != nil {
			if pending.DeliveryID <= 0 {
				return nil, fmt.Errorf("scan pending error log: %w", err)
			}
			pending.InvalidReason = "stored error has invalid field types"
			result = append(result, pending)
			continue
		}

		if stackTrace.Valid {
			value := stackTrace.String
			pending.ErrorLog.StackTrace = &value
		}

		pending.ErrorLog.Metadata = json.RawMessage(metadata)
		if !json.Valid(pending.ErrorLog.Metadata) {
			pending.InvalidReason = "stored error metadata is not valid JSON"
		}

		pending.ErrorLog.CreatedAt, err = parseSQLiteTime(
			createdAt,
		)
		if err != nil {
			pending.InvalidReason = "stored error timestamp is malformed"
		}

		result = append(result, pending)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate pending error logs: %w",
			err,
		)
	}

	return result, nil
}

func (r *QueueRepository) MarkAttempt(
	ctx context.Context,
	deliveryID int64,
	attemptedAt time.Time,
) error {
	return r.MarkAttempts(ctx, []int64{deliveryID}, attemptedAt)
}

func (r *QueueRepository) MarkSent(
	ctx context.Context,
	deliveryID int64,
	sentAt time.Time,
) error {
	return r.MarkSentBatch(ctx, []int64{deliveryID}, sentAt)
}

func parseSQLiteTime(
	value string,
) (time.Time, error) {
	return time.Parse(
		time.RFC3339Nano,
		value,
	)
}

// Quarantine preserves the original payload and source ID. A non-NULL reason is
// the entire quarantine state; there is no separate payload table or retry queue.
func (r *QueueRepository) Quarantine(ctx context.Context, deliveryID int64, reason string) error {
	if deliveryID <= 0 || reason == "" || len(reason) > 512 {
		return fmt.Errorf("invalid quarantine entry")
	}
	_, err := r.db.ExecContext(ctx, `UPDATE delivery_status SET quarantine_reason=?
 WHERE id=? AND sent_at IS NULL AND quarantine_reason IS NULL`, reason, deliveryID)
	if err != nil {
		return fmt.Errorf("quarantine delivery %d: %w", deliveryID, err)
	}
	return nil
}
