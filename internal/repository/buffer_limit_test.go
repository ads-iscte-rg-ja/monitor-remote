package repository_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"monitor-remote/internal/database"
	"monitor-remote/internal/models"
	"monitor-remote/internal/repository"
)

const mib = 1 << 20

// burst writes logs as the SDK does (one error_logs row plus its delivery entry),
// in one transaction to keep the test fast, and returns the new IDs.
func burst(t *testing.T, db *sql.DB, level string, n int) []int64 {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	message := "request failed: upstream timeout after 3000 ms while calling the payments gateway " + strings.Repeat("x", 120)
	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		result, err := tx.Exec(`INSERT INTO error_logs(app_name,environment,level,message,stack_trace,metadata,created_at)
 VALUES('checkout','production',?,?,NULL,'{"request_id":"0123456789abcdef"}',?)`, level, message, now)
		if err != nil {
			t.Fatal(err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO delivery_status(record_type,record_id,queued_at) VALUES('error_log',?,?)`, id, now); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func pendingLogs(t *testing.T, db *sql.DB, level string) []int64 {
	t.Helper()
	rows, err := db.Query(`SELECT el.id FROM error_logs el JOIN delivery_status ds
 ON ds.record_type='error_log' AND ds.record_id=el.id
 WHERE el.level=? AND ds.sent_at IS NULL ORDER BY el.id`, level)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func count(t *testing.T, db *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Oldest first: what remains of a level is the newest part of what was written.
func assertNewestKept(t *testing.T, written, kept []int64) {
	t.Helper()
	if len(kept) > len(written) {
		t.Fatalf("kept %d of %d", len(kept), len(written))
	}
	newest := written[len(written)-len(kept):]
	for i := range kept {
		if kept[i] != newest[i] {
			t.Fatalf("not oldest-first: kept %v, newest written %v", kept, newest)
		}
	}
}

// AV4: a burst of INFO + WARN + ERROR in several cycles while central is down.
// After every cycle the buffer is within the limit, no ERROR is lost and every
// drop is counted and persisted.
func TestBufferLimitAV4Burst(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buffer.db")
	db, err := database.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewQueueRepository(db)
	ctx := context.Background()
	limits := repository.BufferLimits{Soft: 1 * mib, Hard: 4 * mib}
	var infoWritten, warnWritten, errorWritten []int64
	var peak int64
	for cycle := 1; cycle <= 12; cycle++ {
		infoWritten = append(infoWritten, burst(t, db, "INFO", 1000)...)
		warnWritten = append(warnWritten, burst(t, db, "WARN", 50)...)
		errorWritten = append(errorWritten, burst(t, db, "ERROR", 20)...)
		result, err := repo.EnforceBufferLimit(ctx, limits, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		size, err := repo.BufferSize(ctx)
		if err != nil {
			t.Fatal(err)
		}
		peak = max(peak, result.SizeBefore)
		t.Logf("cycle %2d: before=%7d after=%7d limit=%d dropped=%s", cycle, result.SizeBefore, size, limits.Soft, repository.FormatDrops(result.Dropped))
		if size > limits.Soft || result.SizeAfter != size || result.HardLimit {
			t.Fatalf("cycle %d: size %d over the %d limit (%+v)", cycle, size, limits.Soft, result)
		}
	}
	if peak <= limits.Soft {
		t.Fatalf("the burst never reached the limit (peak %d)", peak)
	}

	errorKept := pendingLogs(t, db, "ERROR")
	if len(errorKept) != len(errorWritten) {
		t.Fatalf("ERROR lost: %d of %d kept", len(errorKept), len(errorWritten))
	}
	infoKept := pendingLogs(t, db, "INFO")
	if len(infoKept) == 0 {
		t.Fatal("no recent INFO kept: oldest-first cannot be checked")
	}
	assertNewestKept(t, infoWritten, infoKept)
	if len(pendingLogs(t, db, "WARN")) != len(warnWritten) {
		t.Fatal("WARN dropped while INFO was available")
	}
	drops, err := repo.DroppedByKind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantInfo := int64(len(infoWritten) - len(infoKept))
	if wantInfo == 0 || len(drops) != 1 || drops["INFO"] != wantInfo {
		t.Fatalf("drops %v, want INFO=%d only", drops, wantInfo)
	}
	t.Logf("AV4: written INFO=%d WARN=%d ERROR=%d; dropped %s; ERROR lost=0; peak=%d limit=%d",
		len(infoWritten), len(warnWritten), len(errorWritten), repository.FormatDrops(drops), peak, limits.Soft)

	// The counters survive a restart of the agent.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := database.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	persisted, err := repository.NewQueueRepository(reopened).DroppedByKind(ctx)
	if err != nil || persisted["INFO"] != wantInfo {
		t.Fatalf("persisted drops %v (%v), want INFO=%d", persisted, err, wantInfo)
	}
}

// Above the soft limit: delivered records first (not a loss), then INFO, WARN
// and statistics, oldest first. ERROR+ and quarantine are never touched.
func TestBufferLimitSoftPhaseOrder(t *testing.T) {
	db, repo := prepareQueue(t)
	ctx := context.Background()
	now := time.Now()
	errors := burst(t, db, "ERROR", 50)
	critical := burst(t, db, "CRITICAL", 5)
	quarantined := burst(t, db, "PANIC", 1)
	if err := repo.Quarantine(ctx, deliveryOf(t, db, quarantined[0]), "invalid record"); err != nil {
		t.Fatal(err)
	}
	base, err := repo.BufferSize(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Removing delivered records is enough: nothing pending is dropped.
	delivered := burst(t, db, "INFO", 1500)
	if _, err := db.Exec(`UPDATE delivery_status SET sent_at=? WHERE record_type='error_log' AND record_id BETWEEN ? AND ?`,
		now.UTC().Format(time.RFC3339Nano), delivered[0], delivered[len(delivered)-1]); err != nil {
		t.Fatal(err)
	}
	info := burst(t, db, "INFO", 100)
	size, err := repo.BufferSize(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repo.EnforceBufferLimit(ctx, repository.BufferLimits{Soft: size * 95 / 100, Hard: 4 * mib}, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Delivered == 0 || len(result.Dropped) != 0 || len(pendingLogs(t, db, "INFO")) != len(info) {
		t.Fatalf("delivered records were not removed first: %+v", result)
	}

	// A limit just above the protected records drops every INFO, WARN and statistic.
	warn := burst(t, db, "WARN", 300)
	for i := 0; i < 20; i++ {
		stat := &models.MachineStat{CPUPercent: 1, RAMUsedMB: 1, RAMTotalMB: 2, DiskUsedGB: 1, DiskTotalGB: 2}
		if err := repo.SaveMachineStat(ctx, stat); err != nil {
			t.Fatal(err)
		}
	}
	result, err = repo.EnforceBufferLimit(ctx, repository.BufferLimits{Soft: base, Hard: 4 * mib}, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Dropped["INFO"] != int64(len(info)) || result.Dropped["WARN"] != int64(len(warn)) ||
		result.Dropped["machine_stat"] != 20 || result.HardLimit {
		t.Fatalf("soft phase dropped %v", result.Dropped)
	}
	if len(pendingLogs(t, db, "ERROR")) != len(errors) || len(pendingLogs(t, db, "CRITICAL")) != len(critical) ||
		count(t, db, "SELECT COUNT(*) FROM delivery_status WHERE quarantine_reason IS NOT NULL") != 1 {
		t.Fatal("protected records dropped by the soft phase")
	}
	if n := count(t, db, "SELECT COUNT(*) FROM machine_stats"); n != 0 {
		t.Fatalf("%d statistics payloads left without a queue entry", n)
	}
}

// Above the hard limit: quarantine, then ERROR before CRITICAL before PANIC,
// oldest first, and a CRITICAL event is recorded in the buffer.
func TestBufferLimitHardLimit(t *testing.T) {
	db, repo := prepareQueue(t)
	ctx := context.Background()
	quarantined := burst(t, db, "ERROR", 1)
	if err := repo.Quarantine(ctx, deliveryOf(t, db, quarantined[0]), "invalid record"); err != nil {
		t.Fatal(err)
	}
	burst(t, db, "INFO", 100)
	errors := burst(t, db, "ERROR", 2000)
	critical := burst(t, db, "CRITICAL", 50)
	panics := burst(t, db, "PANIC", 10)
	limits := repository.BufferLimits{Soft: 256 << 10, Hard: 512 << 10}

	result, err := repo.EnforceBufferLimit(ctx, limits, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !result.HardLimit || result.SizeAfter > limits.Hard || result.Dropped["INFO"] != 100 || result.Dropped["ERROR"] < 2 {
		t.Fatalf("hard limit not enforced: %+v", result)
	}
	if count(t, db, "SELECT COUNT(*) FROM delivery_status WHERE quarantine_reason IS NOT NULL") != 0 {
		t.Fatal("quarantine kept above the hard limit")
	}
	assertNewestKept(t, errors, pendingLogs(t, db, "ERROR"))
	if int64(len(errors)-len(pendingLogs(t, db, "ERROR")))+1 != result.Dropped["ERROR"] {
		t.Fatalf("ERROR drops miscounted: %v", result.Dropped)
	}
	if len(pendingLogs(t, db, "PANIC")) != len(panics) {
		t.Fatal("PANIC dropped while ERROR was available")
	}

	var message, metadata string
	if err := db.QueryRow("SELECT message,metadata FROM error_logs WHERE app_name=? AND level='CRITICAL'", repository.BufferEventApp).Scan(&message, &metadata); err != nil {
		t.Fatal("no CRITICAL event:", err)
	}
	var event struct {
		Dropped map[string]int64 `json:"dropped"`
		Limit   int64            `json:"hard_limit_bytes"`
	}
	if err := json.Unmarshal([]byte(metadata), &event); err != nil || event.Limit != limits.Hard || event.Dropped["ERROR"] != result.Dropped["ERROR"] {
		t.Fatalf("event metadata %s (%v)", metadata, err)
	}
	if !strings.Contains(message, "ERROR=") || len(pendingLogs(t, db, "CRITICAL")) != len(critical)+1 {
		t.Fatalf("event %q", message)
	}
}

func TestBufferLimitBelowLimitAndInvalid(t *testing.T) {
	db, repo := prepareQueue(t)
	ctx := context.Background()
	burst(t, db, "INFO", 10)
	result, err := repo.EnforceBufferLimit(ctx, repository.BufferLimits{Soft: mib, Hard: mib}, time.Now())
	if err != nil || result.SizeBefore != result.SizeAfter || len(result.Dropped) != 0 || result.Delivered != 0 {
		t.Fatal(result, err)
	}
	for _, limits := range []repository.BufferLimits{{}, {Soft: mib, Hard: mib - 1}} {
		if _, err := repo.EnforceBufferLimit(ctx, limits, time.Now()); err == nil {
			t.Fatal("invalid limits accepted", limits)
		}
	}
	if drops, err := repo.DroppedByKind(ctx); err != nil || len(drops) != 0 {
		t.Fatal(drops, err)
	}
	if got := repository.FormatDrops(map[string]int64{"WARN": 2, "INFO": 10}); got != "INFO=10 WARN=2" {
		t.Fatal(got)
	}
}

func TestBufferLimitDatabaseErrors(t *testing.T) {
	db, repo := prepareQueue(t)
	ctx := context.Background()
	burst(t, db, "INFO", 10)
	if _, err := db.Exec("DROP TABLE buffer_drops"); err != nil {
		t.Fatal(err)
	}
	limits := repository.BufferLimits{Soft: 1, Hard: mib}
	if _, err := repo.EnforceBufferLimit(ctx, limits, time.Now()); err == nil {
		t.Fatal("a drop that cannot be counted was committed")
	}
	if len(pendingLogs(t, db, "INFO")) != 10 {
		t.Fatal("records dropped without being counted")
	}
	if _, err := repo.DroppedByKind(ctx); err == nil {
		t.Fatal("missing counters table not reported")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.EnforceBufferLimit(ctx, limits, time.Now()); err == nil {
		t.Fatal("closed database not reported")
	}
	if _, err := repo.BufferSize(ctx); err == nil {
		t.Fatal("closed database not reported")
	}
}

func deliveryOf(t *testing.T, db *sql.DB, recordID int64) int64 {
	t.Helper()
	return count(t, db, "SELECT id FROM delivery_status WHERE record_type='error_log' AND record_id=?", recordID)
}
