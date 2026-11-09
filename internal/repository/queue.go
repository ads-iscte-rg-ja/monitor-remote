package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"monitor-remote/internal/models"
)

type QueueRepository struct {
	db *sql.DB
}

func NewQueueRepository(
	db *sql.DB,
) *QueueRepository {
	return &QueueRepository{db: db}
}

func (r *QueueRepository) SaveMachineStat(
	ctx context.Context,
	stat *models.MachineStat,
) error {
	if stat == nil {
		return fmt.Errorf("machine stat is required")
	}

	if stat.CollectedAt.IsZero() {
		stat.CollectedAt = time.Now().UTC()
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf(
			"begin machine stat transaction: %w",
			err,
		)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO machine_stats (
			cpu_percent,
			ram_used_mb,
			ram_total_mb,
			disk_used_gb,
			disk_total_gb,
			collected_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		stat.CPUPercent,
		stat.RAMUsedMB,
		stat.RAMTotalMB,
		stat.DiskUsedGB,
		stat.DiskTotalGB,
		stat.CollectedAt.UTC().Format(
			time.RFC3339Nano,
		),
	)
	if err != nil {
		return fmt.Errorf(
			"insert machine stat: %w",
			err,
		)
	}

	stat.ID, err = result.LastInsertId()
	if err != nil {
		return fmt.Errorf(
			"read machine stat ID: %w",
			err,
		)
	}

	if err := insertDeliveryStatus(
		ctx,
		tx,
		models.RecordTypeMachineStat,
		stat.ID,
	); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf(
			"commit machine stat transaction: %w",
			err,
		)
	}

	return nil
}

func (r *QueueRepository) SaveErrorLog(
	ctx context.Context,
	errorLog *models.ErrorLog,
) error {
	if errorLog == nil {
		return fmt.Errorf("error log is required")
	}

	if errorLog.CreatedAt.IsZero() {
		errorLog.CreatedAt = time.Now().UTC()
	}

	metadata := errorLog.Metadata
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf(
			"begin error log transaction: %w",
			err,
		)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO error_logs (
			app_name,
			environment,
			level,
			message,
			stack_trace,
			metadata,
			created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		errorLog.AppName,
		errorLog.Environment,
		errorLog.Level,
		errorLog.Message,
		errorLog.StackTrace,
		string(metadata),
		errorLog.CreatedAt.UTC().Format(
			time.RFC3339Nano,
		),
	)
	if err != nil {
		return fmt.Errorf(
			"insert error log: %w",
			err,
		)
	}
	errorLog.ID, err = result.LastInsertId()
	if err != nil {
		return fmt.Errorf("read error log ID: %w", err)
	}

	if err := insertDeliveryStatus(
		ctx,
		tx,
		models.RecordTypeErrorLog,
		errorLog.ID,
	); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf(
			"commit error log transaction: %w",
			err,
		)
	}

	return nil
}

func insertDeliveryStatus(
	ctx context.Context,
	tx *sql.Tx,
	recordType string,
	recordID int64,
) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO delivery_status (
			record_type,
			record_id,
			queued_at
		) VALUES (?, ?, ?)`,
		recordType,
		recordID,
		time.Now().UTC().Format(
			time.RFC3339Nano,
		),
	)
	if err != nil {
		return fmt.Errorf(
			"insert delivery status: %w",
			err,
		)
	}

	return nil
}
