package sender

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"monitor-remote/internal/database"
	"monitor-remote/internal/models"
	"monitor-remote/internal/repository"
	"monitor-remote/internal/requests"
)

func TestBadAcknowledgementRetainsRecordAndRetriesSameID(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "buffer.db")
	db, err := database.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := repository.NewQueueRepository(db)
	entry := &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: "failure"}
	if err := repo.SaveErrorLog(ctx, entry); err != nil {
		t.Fatal(err)
	}
	id := entry.ID
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var payload struct {
			Errors []struct {
				ID int64 `json:"id"`
			} `json:"errors"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || len(payload.Errors) != 1 || payload.Errors[0].ID != id {
			t.Error("retry changed source identity", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		if calls == 1 {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": 1, "inserted": 0, "duplicates": 1, "source_ids": []int64{id}})
	}))
	defer server.Close()
	client, err := requests.NewClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	s := New(repo, client)
	if err := s.SendPendingErrorLogs(ctx); err == nil {
		t.Fatal("bad acknowledgement accepted")
	}
	if pending, err := repo.ListPendingErrorLogs(ctx, 10); err != nil || len(pending) != 1 {
		t.Fatal("unacknowledged record was lost", err)
	}
	if err := s.SendPendingErrorLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, err := repo.ListPendingErrorLogs(ctx, 10); err != nil || len(pending) != 0 {
		t.Fatal("duplicate acknowledgement not recorded", err)
	}
}

func TestInvalidRecordsAreQuarantinedWithoutBlockingValidErrors(t *testing.T) {
	ctx := context.Background()
	db, err := database.Open(filepath.Join(t.TempDir(), "quarantine.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := repository.NewQueueRepository(db)
	entries := make([]models.ErrorLog, 6)
	for i := range entries {
		entries[i] = models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: "original payload"}
		if i == 0 {
			entries[i].Message = strings.Repeat("x", 16385)
		}
		if i == 3 {
			entries[i].CreatedAt = time.Now().Add(time.Hour)
		}
		if err := repo.SaveErrorLog(ctx, &entries[i]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("UPDATE error_logs SET metadata='not JSON' WHERE id=?", entries[1].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE error_logs SET created_at='broken timestamp' WHERE id=?", entries[2].ID); err != nil {
		t.Fatal(err)
	}
	seen := make(chan []int64, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Errors []struct {
				ID int64 `json:"id"`
			} `json:"errors"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || len(payload.Errors) == 0 {
			t.Error("bad payload", err)
			w.WriteHeader(500)
			return
		}
		ids := make([]int64, 0, len(payload.Errors))
		rejected := false
		for _, entry := range payload.Errors {
			ids = append(ids, entry.ID)
			if entry.ID == entries[4].ID {
				rejected = true
			}
		}
		seen <- ids
		w.Header().Set("Content-Type", "application/json")
		if rejected {
			w.WriteHeader(400)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid_data", "message": "monitoring field exceeds its size limit", "source_id": entries[4].ID})
			return
		}
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": len(ids), "inserted": len(ids), "duplicates": 0, "source_ids": ids})
	}))
	defer server.Close()
	client, err := requests.NewClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	s := New(repo, client)
	if err := s.SendPendingErrorLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Fatalf("HTTP requests=%d want 2", len(seen))
	}
	first, second := <-seen, <-seen
	if len(first) != 2 || first[0] != entries[5].ID || first[1] != entries[4].ID || len(second) != 1 || second[0] != entries[5].ID {
		t.Fatal("rejection must remove only the named source and retry the remaining batch", first, second)
	}
	var quarantined, sent, payloads int
	if err := db.QueryRow("SELECT COUNT(*) FROM delivery_status WHERE quarantine_reason IS NOT NULL AND sent_at IS NULL").Scan(&quarantined); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NOT NULL").Scan(&sent); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM error_logs").Scan(&payloads); err != nil {
		t.Fatal(err)
	}
	if quarantined != 5 || sent != 1 || payloads != 6 {
		t.Fatalf("quarantined=%d sent=%d payloads=%d", quarantined, sent, payloads)
	}
	var metadata string
	if err := db.QueryRow("SELECT metadata FROM error_logs WHERE id=?", entries[1].ID).Scan(&metadata); err != nil || metadata != "not JSON" {
		t.Fatal("invalid payload was rewritten", err)
	}
	if err := s.SendPendingErrorLogs(ctx); err != nil || len(seen) != 0 {
		t.Fatal("quarantined data was retried", err)
	}
}

func TestCredentialFailurePausesBothCategoriesWithoutQuarantine(t *testing.T) {
	ctx := context.Background()
	db, err := database.Open(filepath.Join(t.TempDir(), "auth.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := repository.NewQueueRepository(db)
	if err := repo.SaveErrorLog(ctx, &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: "failure"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveMachineStat(ctx, &models.MachineStat{CPUPercent: 25, RAMUsedMB: 1, RAMTotalMB: 2, DiskUsedGB: 1, DiskTotalGB: 2}); err != nil {
		t.Fatal(err)
	}
	seen := make(chan struct{}, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen <- struct{}{}; w.WriteHeader(401) }))
	defer server.Close()
	client, err := requests.NewClient(server.URL, "revoked-test-key")
	if err != nil {
		t.Fatal(err)
	}
	s := New(repo, client)
	if err := s.SendPendingErrorLogs(ctx); err == nil {
		t.Fatal("expected credential failure")
	}
	if err := s.SendPendingMachineStats(ctx); !errors.Is(err, errPaused) {
		t.Fatal("shared credential pause not applied", err)
	}
	if len(seen) != 1 {
		t.Fatal("paused delivery performed another HTTP request")
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL AND quarantine_reason IS NULL").Scan(&count); err != nil || count != 2 {
		t.Fatal("valid records were discarded or quarantined", err)
	}
}

func TestStatisticsRetryKeepsPersistentSourceID(t *testing.T) {
	ctx := context.Background()
	db, err := database.Open(filepath.Join(t.TempDir(), "stats.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := repository.NewQueueRepository(db)
	stat := &models.MachineStat{CPUPercent: 25, RAMUsedMB: 1, RAMTotalMB: 2, DiskUsedGB: 1, DiskTotalGB: 2, CollectedAt: time.Now().UTC()}
	if err := repo.SaveMachineStat(ctx, stat); err != nil {
		t.Fatal(err)
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var payload struct {
			Stats []struct {
				ID int64 `json:"id"`
			} `json:"stats"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || len(payload.Stats) != 1 || payload.Stats[0].ID != stat.ID || payload.Stats[0].ID <= 0 {
			t.Error("persistent statistics source ID changed", err)
		}
		ackID := stat.ID
		if calls == 1 {
			ackID++
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]int64{{"id": 99, "host_id": 7, "source_id": ackID}}})
	}))
	defer server.Close()
	client, err := requests.NewClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	s := New(repo, client)
	if err := s.SendPendingMachineStats(ctx); err == nil {
		t.Fatal("mismatched source ID accepted")
	}
	if pending, err := repo.ListPendingMachineStats(ctx, 10); err != nil || len(pending) != 1 {
		t.Fatal("unacknowledged statistics lost", err)
	}
	if err := s.SendPendingMachineStats(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, err := repo.ListPendingMachineStats(ctx, 10); err != nil || len(pending) != 0 {
		t.Fatal("valid statistics acknowledgement not persisted", err)
	}
}

func TestFlushSendsAllPendingRecordsInNewestFirstBatches(t *testing.T) {
	for _, kind := range []string{"stats", "errors"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			db, err := database.Open(filepath.Join(t.TempDir(), "batch.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			repo := repository.NewQueueRepository(db)
			for i := 0; i < 101; i++ {
				if kind == "stats" {
					err = repo.SaveMachineStat(ctx, &models.MachineStat{CPUPercent: 25, RAMUsedMB: 1, RAMTotalMB: 2, DiskUsedGB: 1, DiskTotalGB: 2})
				} else {
					err = repo.SaveErrorLog(ctx, &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: "failure"})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			// Also exercise the descending ID tie-breaker.
			if _, err := db.Exec("UPDATE delivery_status SET queued_at='2026-01-01T00:00:00Z'"); err != nil {
				t.Fatal(err)
			}
			seen := make(chan []int64, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload struct {
					Stats []struct {
						ID int64 `json:"id"`
					} `json:"stats"`
					Errors []struct {
						ID int64 `json:"id"`
					} `json:"errors"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return
				}
				ids := make([]int64, 0, 100)
				data := make([]map[string]int64, 0, 100)
				for _, entry := range payload.Stats {
					ids = append(ids, entry.ID)
					data = append(data, map[string]int64{"id": entry.ID, "host_id": 7, "source_id": entry.ID})
				}
				for _, entry := range payload.Errors {
					ids = append(ids, entry.ID)
				}
				seen <- ids
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(201)
				if kind == "stats" {
					_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
				} else {
					_ = json.NewEncoder(w).Encode(map[string]any{"accepted": len(ids), "inserted": len(ids), "duplicates": 0, "source_ids": ids})
				}
			}))
			defer server.Close()
			client, err := requests.NewClient(server.URL, "test-key")
			if err != nil {
				t.Fatal(err)
			}
			s := New(repo, client)
			if kind == "stats" {
				err = s.SendPendingMachineStats(ctx)
			} else {
				err = s.SendPendingErrorLogs(ctx)
			}
			if err != nil || len(seen) != 2 {
				t.Fatal("expected consecutive 100-record and 1-record requests", err)
			}
			ids := <-seen
			if len(ids) != 100 {
				t.Fatal("full batch not submitted", len(ids))
			}
			for i, id := range ids {
				if id != int64(101-i) {
					t.Fatal("batch is not newest first", ids)
				}
			}
			last := <-seen
			if len(last) != 1 || last[0] != 1 {
				t.Fatal("flush did not reach the oldest record", last)
			}
			var pending int
			if err := db.QueryRow("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL").Scan(&pending); err != nil || pending != 0 {
				t.Fatal("cycle left an accepted backlog pending", pending, err)
			}
			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NOT NULL").Scan(&count); err != nil || count != 101 {
				t.Fatal("complete batch was not marked delivered", count, err)
			}
		})
	}
}

func TestFlushLeavesConcurrentWritesForNextCycle(t *testing.T) {
	ctx := context.Background()
	db, err := database.Open(filepath.Join(t.TempDir(), "snapshot.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := repository.NewQueueRepository(db)
	save := func() error {
		return repo.SaveErrorLog(ctx, &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: "failure"})
	}
	if err := save(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	seen := make(chan int64, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Errors []struct {
				ID int64 `json:"id"`
			} `json:"errors"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || len(payload.Errors) != 1 {
			t.Error("invalid payload", err)
			w.WriteHeader(500)
			return
		}
		once.Do(func() {
			if err := save(); err != nil {
				t.Error(err)
			}
		})
		id := payload.Errors[0].ID
		seen <- id
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": 1, "inserted": 1, "duplicates": 0, "source_ids": []int64{id}})
	}))
	defer server.Close()
	client, err := requests.NewClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	s := New(repo, client)
	if err := s.SendPendingErrorLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || <-seen != 1 {
		t.Fatal("flush chased writes beyond its snapshot")
	}
	pending, err := repo.ListPendingErrorLogs(ctx, 100)
	if err != nil || len(pending) != 1 || pending[0].ErrorLog.ID != 2 {
		t.Fatal("concurrent write was lost", pending, err)
	}
	if err := s.SendPendingErrorLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || <-seen != 2 {
		t.Fatal("next timer did not flush the newly accepted record")
	}
}

func TestFlushStopsOnFailureAndRetriesOnlyRemainingBacklog(t *testing.T) {
	ctx := context.Background()
	db, err := database.Open(filepath.Join(t.TempDir(), "flush-retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := repository.NewQueueRepository(db)
	for i := 0; i < 201; i++ {
		if err := repo.SaveErrorLog(ctx, &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: "failure"}); err != nil {
			t.Fatal(err)
		}
	}
	calls := make(chan []int64, 6)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Errors []struct {
				ID int64 `json:"id"`
			} `json:"errors"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		ids := make([]int64, 0, len(payload.Errors))
		for _, entry := range payload.Errors {
			ids = append(ids, entry.ID)
		}
		calls <- ids
		if len(calls) == 2 {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": len(ids), "inserted": len(ids), "duplicates": 0, "source_ids": ids})
	}))
	defer server.Close()
	client, err := requests.NewClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	s := New(repo, client)
	if err := s.SendPendingErrorLogs(ctx); err == nil || len(calls) != 2 {
		t.Fatal("failure did not stop the flush", err)
	}
	var pending int
	if err := db.QueryRow("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL AND quarantine_reason IS NULL").Scan(&pending); err != nil || pending != 101 {
		t.Fatal("remaining backlog was not preserved", pending, err)
	}
	if err := s.SendPendingErrorLogs(ctx); err != nil || len(calls) != 4 {
		t.Fatal("retry did not drain the remaining two batches", err)
	}
	first, failed, retried, last := <-calls, <-calls, <-calls, <-calls
	if len(first) != 100 || first[0] != 201 || len(failed) != 100 || failed[0] != 101 || len(retried) != 100 || retried[0] != 101 || len(last) != 1 || last[0] != 1 {
		t.Fatal("retry changed batch identities or replayed the acknowledged batch")
	}
}

func TestOversizedCombinedPayloadSplitsWithoutQuarantine(t *testing.T) {
	ctx := context.Background()
	db, err := database.Open(filepath.Join(t.TempDir(), "large-batch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := repository.NewQueueRepository(db)
	stack := strings.Repeat("s", 65536)
	for i := 0; i < 24; i++ {
		if err := repo.SaveErrorLog(ctx, &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: strings.Repeat("m", 16384), StackTrace: &stack}); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(chan []int64, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1048577))
		if err != nil || len(body) > 1048576 {
			t.Error("request exceeded body limit", err)
			w.WriteHeader(500)
			return
		}
		var payload struct {
			Errors []struct {
				ID int64 `json:"id"`
			} `json:"errors"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		ids := make([]int64, 0, len(payload.Errors))
		for _, entry := range payload.Errors {
			ids = append(ids, entry.ID)
		}
		seen <- ids
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": len(ids), "inserted": len(ids), "duplicates": 0, "source_ids": ids})
	}))
	defer server.Close()
	client, err := requests.NewClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	if err := New(repo, client).SendPendingErrorLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Fatal("expected two size-bounded requests", len(seen))
	}
	ids := append(<-seen, (<-seen)...)
	if len(ids) != 24 {
		t.Fatal("split lost records", ids)
	}
	for i, id := range ids {
		if id != int64(24-i) {
			t.Fatal("split changed newest-first ordering", ids)
		}
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NOT NULL AND quarantine_reason IS NULL").Scan(&count); err != nil || count != 24 {
		t.Fatal("valid records were quarantined or retained", count, err)
	}
}

func TestUnidentifiedBatchRejectionNeverQuarantinesMembers(t *testing.T) {
	for _, body := range []string{
		`{"error":"invalid_data","message":"timestamp cannot be in the future","source_id":999}`,
		`{"error":"invalid_data","message":"timestamp cannot be in the future"}`,
	} {
		ctx := context.Background()
		db, err := database.Open(filepath.Join(t.TempDir(), "unknown-rejection.db"))
		if err != nil {
			t.Fatal(err)
		}
		repo := repository.NewQueueRepository(db)
		for i := 0; i < 2; i++ {
			if err := repo.SaveErrorLog(ctx, &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: "failure"}); err != nil {
				t.Fatal(err)
			}
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(400)
			_, _ = w.Write([]byte(body))
		}))
		client, err := requests.NewClient(server.URL, "test-key")
		if err != nil {
			t.Fatal(err)
		}
		s := New(repo, client)
		if err := s.SendPendingErrorLogs(ctx); err == nil {
			t.Fatal("unidentified rejection accepted")
		}
		if !errors.Is(s.deliveryGate(), errPaused) {
			t.Fatal("unidentified rejection must pause the incompatible delivery contract")
		}
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL AND quarantine_reason IS NULL").Scan(&count); err != nil || count != 2 {
			t.Fatal("batch was quarantined or acknowledged", count, err)
		}
		server.Close()
		_ = db.Close()
	}
}
