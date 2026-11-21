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
