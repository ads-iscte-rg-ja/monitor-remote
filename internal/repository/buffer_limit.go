package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"monitor-remote/internal/models"
)

// BufferLimits bounds the SQLite buffer in bytes (ADR-0009). Above Soft, only
// delivered records, INFO, WARN and machine statistics are removed; ERROR and
// above are removed only above Hard.
type BufferLimits struct {
	Soft int64
	Hard int64
}

// BufferEnforcement reports one enforcement cycle. Delivered records removed
// early are not a loss (central has them) and are not counted as drops.
type BufferEnforcement struct {
	SizeBefore int64
	SizeAfter  int64
	Delivered  int
	Dropped    map[string]int64
	HardLimit  bool
}

const (
	// One short transaction per batch keeps SDK writers waiting little.
	bufferDropBatch = 500
	// Shedding stops at 90 % of the limit so that it does not run every cycle.
	bufferTargetPercent = 90

	// BufferEventApp is the application name of the CRITICAL event recorded
	// when ERROR+ records are dropped.
	BufferEventApp = "monitor-remote"
)

// Each query returns queue ID, record type, record ID and the drop counter key,
// oldest first; an empty key marks a delivered record (not a loss).
const (
	selectDelivered = `SELECT id,record_type,record_id,'' FROM delivery_status
 WHERE sent_at IS NOT NULL ORDER BY sent_at,id LIMIT ?`
	selectPendingLevel = `SELECT ds.id,ds.record_type,ds.record_id,el.level FROM error_logs el
 JOIN delivery_status ds ON ds.record_type='error_log' AND ds.record_id=el.id
 WHERE el.level=? AND ds.sent_at IS NULL AND ds.quarantine_reason IS NULL
 ORDER BY el.id LIMIT ?`
	selectPendingStats = `SELECT id,record_type,record_id,'machine_stat' FROM delivery_status
 WHERE record_type='machine_stat' AND sent_at IS NULL AND quarantine_reason IS NULL
 ORDER BY queued_at,id LIMIT ?`
	selectQuarantined = `SELECT ds.id,ds.record_type,ds.record_id,COALESCE(el.level,'machine_stat')
 FROM delivery_status ds
 LEFT JOIN error_logs el ON ds.record_type='error_log' AND el.id=ds.record_id
 WHERE ds.quarantine_reason IS NOT NULL ORDER BY ds.id LIMIT ?`
)

type shedPhase struct {
	query string
	args  []any
}

var (
	softPhases = []shedPhase{
		{query: selectDelivered},
		{query: selectPendingLevel, args: []any{"INFO"}},
		{query: selectPendingLevel, args: []any{"WARN"}},
		{query: selectPendingStats},
	}
	hardPhases = []shedPhase{
		{query: selectQuarantined},
		{query: selectPendingLevel, args: []any{"ERROR"}},
		{query: selectPendingLevel, args: []any{"CRITICAL"}},
		{query: selectPendingLevel, args: []any{"PANIC"}},
	}
)

// BufferSize is the space in use: pages minus free pages. The file itself does
// not shrink (auto_vacuum is off), but freed pages are reused.
func (r *QueueRepository) BufferSize(ctx context.Context) (int64, error) {
	var size int64
	err := r.db.QueryRowContext(ctx, `SELECT (p.page_count-f.freelist_count)*s.page_size
 FROM pragma_page_count() p, pragma_freelist_count() f, pragma_page_size() s`).Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("measure buffer size: %w", err)
	}
	return size, nil
}

// EnforceBufferLimit applies the severity-ordered drop policy of ADR-0009.
func (r *QueueRepository) EnforceBufferLimit(ctx context.Context, limits BufferLimits, now time.Time) (BufferEnforcement, error) {
	result := BufferEnforcement{Dropped: map[string]int64{}}
	if limits.Soft <= 0 || limits.Hard < limits.Soft {
		return result, fmt.Errorf("invalid buffer limits")
	}
	size, err := r.BufferSize(ctx)
	if err != nil {
		return result, err
	}
	result.SizeBefore, result.SizeAfter = size, size
	if size > limits.Soft {
		if size, err = r.shed(ctx, softPhases, limits.Soft*bufferTargetPercent/100, size, now, &result); err != nil {
			return result, err
		}
	}
	if size > limits.Hard {
		if size, err = r.enforceHardLimit(ctx, limits, size, now, &result); err != nil {
			return result, err
		}
	}
	result.SizeAfter = size
	return result, nil
}

// enforceHardLimit drops ERROR+ and quarantined records and, if it dropped any,
// records the CRITICAL event.
func (r *QueueRepository) enforceHardLimit(ctx context.Context, limits BufferLimits, size int64, now time.Time, result *BufferEnforcement) (int64, error) {
	before := totalDropped(result.Dropped)
	size, err := r.shed(ctx, hardPhases, limits.Hard*bufferTargetPercent/100, size, now, result)
	if err != nil || totalDropped(result.Dropped) == before {
		return size, err
	}
	result.HardLimit = true
	if err := r.recordHardLimitEvent(ctx, limits, *result, now); err != nil {
		return size, err
	}
	return r.BufferSize(ctx)
}

// DroppedByKind returns the persisted drop counters.
func (r *QueueRepository) DroppedByKind(ctx context.Context) (map[string]int64, error) {
	rows, err := r.db.QueryContext(ctx, "SELECT kind,dropped FROM buffer_drops")
	if err != nil {
		return nil, fmt.Errorf("read buffer drops: %w", err)
	}
	defer rows.Close()
	counts := map[string]int64{}
	for rows.Next() {
		var kind string
		var dropped int64
		if err := rows.Scan(&kind, &dropped); err != nil {
			return nil, err
		}
		counts[kind] = dropped
	}
	return counts, rows.Err()
}

func (r *QueueRepository) shed(ctx context.Context, phases []shedPhase, target, size int64, now time.Time, result *BufferEnforcement) (int64, error) {
	for _, phase := range phases {
		for size > target {
			removed, err := r.dropBatch(ctx, phase, now, result)
			if err != nil {
				return size, err
			}
			if removed == 0 {
				break
			}
			if size, err = r.BufferSize(ctx); err != nil {
				return size, err
			}
		}
	}
	return size, nil
}

type bufferEntry struct {
	queueID, recordID int64
	recordType, kind  string
}

// dropBatch removes one batch of the phase and counts it in one transaction.
func (r *QueueRepository) dropBatch(ctx context.Context, phase shedPhase, now time.Time, result *BufferEnforcement) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	entries, err := selectEntries(ctx, tx, phase)
	if err != nil {
		return 0, err
	}
	counts := map[string]int64{}
	for _, entry := range entries {
		if err := deleteEntry(ctx, tx, entry); err != nil {
			return 0, err
		}
		if entry.kind != "" {
			counts[entry.kind]++
		}
	}
	at := now.UTC().Format(time.RFC3339Nano)
	for kind, n := range counts {
		if _, err := tx.ExecContext(ctx, `INSERT INTO buffer_drops(kind,dropped,last_dropped_at) VALUES(?,?,?)
 ON CONFLICT(kind) DO UPDATE SET dropped=dropped+excluded.dropped,last_dropped_at=excluded.last_dropped_at`,
			kind, n, at); err != nil {
			return 0, fmt.Errorf("count buffer drops: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	for kind, n := range counts {
		result.Dropped[kind] += n
	}
	result.Delivered += len(entries) - int(totalDropped(counts))
	return len(entries), nil
}

func selectEntries(ctx context.Context, tx *sql.Tx, phase shedPhase) ([]bufferEntry, error) {
	rows, err := tx.QueryContext(ctx, phase.query, append(append([]any{}, phase.args...), bufferDropBatch)...)
	if err != nil {
		return nil, fmt.Errorf("select buffer entries: %w", err)
	}
	defer rows.Close()
	var entries []bufferEntry
	for rows.Next() {
		var entry bufferEntry
		if err := rows.Scan(&entry.queueID, &entry.recordType, &entry.recordID, &entry.kind); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func deleteEntry(ctx context.Context, tx *sql.Tx, entry bufferEntry) error {
	var deletePayload string
	switch entry.recordType {
	case models.RecordTypeMachineStat:
		deletePayload = "DELETE FROM machine_stats WHERE id=?"
	case models.RecordTypeErrorLog:
		deletePayload = "DELETE FROM error_logs WHERE id=?"
	default:
		return fmt.Errorf("invalid buffered record type")
	}
	if _, err := tx.ExecContext(ctx, deletePayload, entry.recordID); err != nil {
		return fmt.Errorf("drop buffered record: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM delivery_status WHERE id=?", entry.queueID); err != nil {
		return fmt.Errorf("drop buffer entry: %w", err)
	}
	return nil
}

// recordHardLimitEvent stores a CRITICAL log in the buffer itself, so the loss
// of ERROR+ records reaches central and its error_level alert rules.
func (r *QueueRepository) recordHardLimitEvent(ctx context.Context, limits BufferLimits, result BufferEnforcement, now time.Time) error {
	metadata, err := json.Marshal(map[string]any{
		"dropped":          result.Dropped,
		"size_bytes":       result.SizeBefore,
		"hard_limit_bytes": limits.Hard,
	})
	if err != nil {
		return err
	}
	event := &models.ErrorLog{
		AppName:     BufferEventApp,
		Environment: "agent",
		Level:       "CRITICAL",
		Message: fmt.Sprintf("buffer hard limit of %d MiB reached; dropped %s",
			limits.Hard>>20, FormatDrops(result.Dropped)),
		Metadata:  metadata,
		CreatedAt: now.UTC(),
	}
	if err := r.SaveErrorLog(ctx, event); err != nil {
		return fmt.Errorf("record buffer hard-limit event: %w", err)
	}
	return nil
}

// FormatDrops renders counters in a stable order, e.g. "ERROR=3 INFO=10".
func FormatDrops(counts map[string]int64) string {
	kinds := make([]string, 0, len(counts))
	for kind := range counts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, fmt.Sprintf("%s=%d", kind, counts[kind]))
	}
	return strings.Join(parts, " ")
}

func totalDropped(counts map[string]int64) int64 {
	var total int64
	for _, n := range counts {
		total += n
	}
	return total
}
