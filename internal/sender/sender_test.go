package sender

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ingestv1 "github.com/ads-iscte-rg-ja/monitor-api/gen/go/monitor/ingest/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"monitor-remote/internal/database"
	"monitor-remote/internal/models"
	"monitor-remote/internal/repository"
	"monitor-remote/internal/requests/requeststest"
)

func openRepo(t *testing.T, name string) (*repository.QueueRepository, func(query string, args ...any) int) {
	t.Helper()
	db, err := database.Open(filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		if err := db.QueryRow(query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	return repository.NewQueueRepository(db), count
}

func saveError(t *testing.T, repo *repository.QueueRepository, entry *models.ErrorLog) {
	t.Helper()
	if entry == nil {
		entry = &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: "failure"}
	}
	if err := repo.SaveErrorLog(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
}

func saveStat(t *testing.T, repo *repository.QueueRepository) *models.MachineStat {
	t.Helper()
	stat := &models.MachineStat{CPUPercent: 25, RAMUsedMB: 1, RAMTotalMB: 2, DiskUsedGB: 1, DiskTotalGB: 2, CollectedAt: time.Now().UTC()}
	if err := repo.SaveMachineStat(context.Background(), stat); err != nil {
		t.Fatal(err)
	}
	return stat
}

func TestBadAcknowledgementRetainsRecordAndRetriesSameID(t *testing.T) {
	ctx := context.Background()
	repo, _ := openRepo(t, "buffer.db")
	entry := &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: "failure"}
	saveError(t, repo, entry)
	calls := 0
	central := &requeststest.Central{Errors: func(r *ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
		calls++
		if ids := requeststest.ErrorIDs(r); len(ids) != 1 || ids[0] != entry.ID {
			t.Error("retry changed source identity", ids)
		}
		if calls == 1 {
			return &ingestv1.IngestErrorsResponse{}, nil
		}
		return requeststest.AckErrors(r, 0), nil // already stored: duplicate
	}}
	s := New(repo, requeststest.Start(t, central, "test-key"))
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
		saveError(t, repo, &entries[i])
	}
	if _, err := db.Exec("UPDATE error_logs SET metadata='not JSON' WHERE id=?", entries[1].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE error_logs SET created_at='broken timestamp' WHERE id=?", entries[2].ID); err != nil {
		t.Fatal(err)
	}
	seen := make(chan []int64, 10)
	central := &requeststest.Central{Errors: func(r *ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
		ids := requeststest.ErrorIDs(r)
		seen <- ids
		for _, id := range ids {
			if id == entries[4].ID {
				return nil, requeststest.Reject(id, ingestv1.RejectionReason_REJECTION_REASON_FIELD_TOO_LARGE)
			}
		}
		return requeststest.AckErrors(r, len(ids)), nil
	}}
	s := New(repo, requeststest.Start(t, central, "test-key"))
	if err := s.SendPendingErrorLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Fatalf("requests=%d want 2", len(seen))
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
	var reason string
	if err := db.QueryRow("SELECT quarantine_reason FROM delivery_status WHERE record_id=?", entries[4].ID).Scan(&reason); err != nil || reason != "central rejected record: field_too_large" {
		t.Fatalf("quarantine reason must come from the contract code: %q %v", reason, err)
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
	repo, count := openRepo(t, "auth.db")
	saveError(t, repo, nil)
	saveStat(t, repo)
	unauthenticated := status.Error(codes.Unauthenticated, "invalid or missing API key")
	central := &requeststest.Central{
		Errors: func(*ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
			return nil, unauthenticated
		},
		Stats: func(*ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) { return nil, unauthenticated },
	}
	s := New(repo, requeststest.Start(t, central, "revoked-test-key"))
	if err := s.SendPendingErrorLogs(ctx); err == nil {
		t.Fatal("expected credential failure")
	}
	if err := s.SendPendingMachineStats(ctx); !errors.Is(err, errPaused) {
		t.Fatal("shared credential pause not applied", err)
	}
	if central.Calls() != 1 {
		t.Fatal("paused delivery performed another request")
	}
	if n := count("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL AND quarantine_reason IS NULL"); n != 2 {
		t.Fatal("valid records were discarded or quarantined", n)
	}
}

func TestStatisticsRetryKeepsPersistentSourceID(t *testing.T) {
	ctx := context.Background()
	repo, _ := openRepo(t, "stats.db")
	stat := saveStat(t, repo)
	calls := 0
	central := &requeststest.Central{Stats: func(r *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
		calls++
		if ids := requeststest.StatIDs(r); len(ids) != 1 || ids[0] != stat.ID || ids[0] <= 0 {
			t.Error("persistent statistics source ID changed", ids)
		}
		ackID := stat.ID
		if calls == 1 {
			ackID++
		}
		return &ingestv1.IngestStatsResponse{Acknowledgements: []*ingestv1.StatAcknowledgement{{Id: 99, HostId: 7, SourceId: ackID}}}, nil
	}}
	s := New(repo, requeststest.Start(t, central, "test-key"))
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
					saveStat(t, repo)
				} else {
					saveError(t, repo, nil)
				}
			}
			// Also exercise the descending ID tie-breaker.
			if _, err := db.Exec("UPDATE delivery_status SET queued_at='2026-01-01T00:00:00Z'"); err != nil {
				t.Fatal(err)
			}
			seen := make(chan []int64, 2)
			central := &requeststest.Central{
				Stats: func(r *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
					seen <- requeststest.StatIDs(r)
					return requeststest.AckStats(r, 7), nil
				},
				Errors: func(r *ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
					seen <- requeststest.ErrorIDs(r)
					return requeststest.AckErrors(r, len(r.GetErrors())), nil
				},
			}
			s := New(repo, requeststest.Start(t, central, "test-key"))
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
			var pending, sent int
			if err := db.QueryRow("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL").Scan(&pending); err != nil || pending != 0 {
				t.Fatal("cycle left an accepted backlog pending", pending, err)
			}
			if err := db.QueryRow("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NOT NULL").Scan(&sent); err != nil || sent != 101 {
				t.Fatal("complete batch was not marked delivered", sent, err)
			}
		})
	}
}

func TestFlushLeavesConcurrentWritesForNextCycle(t *testing.T) {
	ctx := context.Background()
	repo, _ := openRepo(t, "snapshot.db")
	saveError(t, repo, nil)
	var once sync.Once
	seen := make(chan int64, 4)
	central := &requeststest.Central{Errors: func(r *ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
		ids := requeststest.ErrorIDs(r)
		if len(ids) != 1 {
			t.Error("invalid payload", ids)
		}
		once.Do(func() { saveError(t, repo, nil) })
		seen <- ids[0]
		return requeststest.AckErrors(r, 1), nil
	}}
	s := New(repo, requeststest.Start(t, central, "test-key"))
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
	repo, count := openRepo(t, "flush-retry.db")
	for i := 0; i < 201; i++ {
		saveError(t, repo, nil)
	}
	calls := make(chan []int64, 6)
	central := &requeststest.Central{Errors: func(r *ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
		calls <- requeststest.ErrorIDs(r)
		if len(calls) == 2 {
			return nil, status.Error(codes.Unavailable, "central restarting")
		}
		return requeststest.AckErrors(r, len(r.GetErrors())), nil
	}}
	s := New(repo, requeststest.Start(t, central, "test-key"))
	if err := s.SendPendingErrorLogs(ctx); err == nil || len(calls) != 2 {
		t.Fatal("failure did not stop the flush", err)
	}
	if n := count("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL AND quarantine_reason IS NULL"); n != 101 {
		t.Fatal("remaining backlog was not preserved", n)
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
	repo, count := openRepo(t, "large-batch.db")
	stack := strings.Repeat("s", 65536)
	for i := 0; i < 24; i++ {
		saveError(t, repo, &models.ErrorLog{AppName: "app", Environment: "test", Level: "ERROR", Message: strings.Repeat("m", 16384), StackTrace: &stack})
	}
	seen := make(chan []int64, 10)
	central := &requeststest.Central{Errors: func(r *ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
		seen <- requeststest.ErrorIDs(r)
		return requeststest.AckErrors(r, len(r.GetErrors())), nil
	}}
	if err := New(repo, requeststest.Start(t, central, "test-key")).SendPendingErrorLogs(ctx); err != nil {
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
	if n := count("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NOT NULL AND quarantine_reason IS NULL"); n != 24 {
		t.Fatal("valid records were quarantined or retained", n)
	}
}

func TestUnidentifiedBatchRejectionNeverQuarantinesMembers(t *testing.T) {
	for name, rejection := range map[string]error{
		"foreign source": requeststest.Reject(999, ingestv1.RejectionReason_REJECTION_REASON_FUTURE_TIMESTAMP),
		"no source":      status.Error(codes.InvalidArgument, "statistics batch cannot be empty"),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			repo, count := openRepo(t, "unknown-rejection.db")
			saveError(t, repo, nil)
			saveError(t, repo, nil)
			central := &requeststest.Central{Errors: func(*ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) { return nil, rejection }}
			s := New(repo, requeststest.Start(t, central, "test-key"))
			if err := s.SendPendingErrorLogs(ctx); err == nil {
				t.Fatal("unidentified rejection accepted")
			}
			if !errors.Is(s.deliveryGate(), errPaused) {
				t.Fatal("unidentified rejection must pause the incompatible delivery contract")
			}
			if n := count("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL AND quarantine_reason IS NULL"); n != 2 {
				t.Fatal("batch was quarantined or acknowledged", n)
			}
		})
	}
}
