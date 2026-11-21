package sender

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	ingestv1 "github.com/ads-iscte-rg-ja/monitor-api/gen/go/monitor/ingest/v1"
	"google.golang.org/protobuf/proto"

	"monitor-remote/internal/database"
	"monitor-remote/internal/repository"
	"monitor-remote/internal/requests/requeststest"
)

// MN1 (remote side): the quarantine and the rest of the buffer state go with
// every statistics batch of the cycle, as one snapshot taken at its start.
func TestStatisticsCarryTheBufferSnapshot(t *testing.T) {
	ctx := context.Background()
	repo, _ := openRepo(t, "status.db")
	for i := 0; i < 150; i++ { // two batches of at most 100
		saveStat(t, repo)
	}
	saveError(t, repo, nil)
	saveError(t, repo, nil)
	quarantined, err := repo.ListPendingErrorLogs(ctx, 1)
	if err != nil || len(quarantined) != 1 {
		t.Fatal(quarantined, err)
	}
	if err := repo.Quarantine(ctx, quarantined[0].DeliveryID, "invalid record"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var seen []*ingestv1.BufferStatus
	central := &requeststest.Central{Stats: func(r *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
		mu.Lock()
		seen = append(seen, r.GetBuffer())
		mu.Unlock()
		return requeststest.AckStats(r, 7), nil
	}}
	before := time.Now()
	if err := New(repo, requeststest.Start(t, central, "test-key")).SendPendingMachineStats(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Fatalf("%d batches", len(seen))
	}
	first := seen[0]
	if first == nil || !proto.Equal(first, seen[1]) {
		t.Fatalf("each batch of the cycle must carry the same snapshot: %v / %v", first, seen[1])
	}
	// 150 statistics and 1 error pending, 1 error in quarantine.
	if first.GetPending() != 151 || first.GetQuarantined() != 1 || first.GetDeliveryPaused() ||
		first.GetSizeBytes() <= 0 || first.GetObservedAt().AsTime().Before(before.Add(-time.Second)) {
		t.Fatalf("snapshot %v", first)
	}
	if len(first.GetDroppedByKind()) != 0 {
		t.Fatalf("no drops expected: %v", first.GetDroppedByKind())
	}
}

func TestStatisticsAreSentWithoutSnapshotWhenItFails(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "nostatus.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := repository.NewQueueRepository(db)
	saveStat(t, repo)
	// A broken counters table makes the snapshot fail; delivery must not.
	if _, err := db.Exec("DROP TABLE buffer_drops"); err != nil {
		t.Fatal(err)
	}
	var got *ingestv1.BufferStatus
	sent := false
	central := &requeststest.Central{Stats: func(r *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
		got, sent = r.GetBuffer(), true
		return requeststest.AckStats(r, 7), nil
	}}
	if err := New(repo, requeststest.Start(t, central, "test-key")).SendPendingMachineStats(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !sent || got != nil {
		t.Fatalf("statistics must be sent without a snapshot: sent=%v buffer=%v", sent, got)
	}
}

// MN1 end to end found that a statistics cycle with nothing pending sent no
// snapshot, leaving central one cycle behind (ADR-0012). Such a cycle now sends
// the next stored statistic at once, with a snapshot that sees the quarantine.
func TestIdleStatisticsCycleSendsTheNextStatisticAtOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	repo, _ := openRepo(t, "idle.db")
	seen := make(chan *ingestv1.BufferStatus, 4)
	central := &requeststest.Central{Stats: func(r *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
		seen <- r.GetBuffer()
		return requeststest.AckStats(r, 7), nil
	}}
	s := New(repo, requeststest.Start(t, central, "test-key"))

	done := make(chan error, 1)
	go func() { done <- s.deliverMachineStats(ctx, time.Hour) }()
	saveError(t, repo, nil)
	pending, err := repo.ListPendingErrorLogs(ctx, 1)
	if err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
	}
	if err := repo.Quarantine(ctx, pending[0].DeliveryID, "invalid record"); err != nil {
		t.Fatal(err)
	}
	saveStat(t, repo)
	s.NotifyStatsQueued()

	// Well before the one-hour interval: the cycle does not wait for its timer.
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case status := <-seen:
		if status.GetQuarantined() != 1 {
			t.Fatalf("snapshot %v", status)
		}
	default:
		t.Fatal("the statistic was not sent in this cycle")
	}
}

func TestIdleStatisticsCycleWaitsAtMostOneInterval(t *testing.T) {
	repo, _ := openRepo(t, "quiet.db")
	requests := 0
	central := &requeststest.Central{Stats: func(r *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
		requests++
		return requeststest.AckStats(r, 7), nil
	}}
	s := New(repo, requeststest.Start(t, central, "test-key"))
	if err := s.deliverMachineStats(context.Background(), 10*time.Millisecond); err != nil || requests != 0 {
		t.Fatalf("err=%v requests=%d", err, requests)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.deliverMachineStats(ctx, time.Hour); err == nil {
		t.Fatal("a cancelled cycle must report the context error")
	}
	s.NotifyStatsQueued()
	s.NotifyStatsQueued() // never blocks when a token is already waiting
}

func TestStatisticsWorkerDrainsTheNotificationAndStopsWithItsContext(t *testing.T) {
	repo, _ := openRepo(t, "worker.db")
	sent := make(chan struct{}, 8)
	central := &requeststest.Central{Stats: func(r *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
		sent <- struct{}{}
		return requeststest.AckStats(r, 7), nil
	}}
	s := New(repo, requeststest.Start(t, central, "test-key"))
	saveStat(t, repo)
	s.NotifyStatsQueued() // the cycle sends this statistic and consumes the token

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.RunMachineStats(ctx, time.Hour, func(err error) { t.Error(err) }); close(done) }()
	<-sent
	cancel()
	<-done
	if len(s.statsQueued) != 0 {
		t.Fatal("the notification for a statistic already sent must not trigger another cycle")
	}

	// An idle cycle (empty buffer) cancelled while it waits for the next statistic.
	empty, _ := openRepo(t, "empty.db")
	waiting, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	if err := New(empty, requeststest.Start(t, central, "test-key")).deliverMachineStats(waiting, time.Hour); err == nil {
		t.Fatal("a cycle cancelled while waiting must report the context error")
	}
}
