package requests_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ingestv1 "github.com/ads-iscte-rg-ja/monitor-api/gen/go/monitor/ingest/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"monitor-remote/internal/models"
	"monitor-remote/internal/requests"
	"monitor-remote/internal/requests/requeststest"
	"monitor-remote/internal/validation"
)

func validStat(id int64) models.MachineStat {
	return models.MachineStat{ID: id, CPUPercent: 25, RAMUsedMB: 1, RAMTotalMB: 2, DiskUsedGB: 1, DiskTotalGB: 2, CollectedAt: time.Now().UTC().Add(-time.Second)}
}

func validError(id int64) models.ErrorLog {
	return models.ErrorLog{ID: id, AppName: "app", Environment: "test", Level: "ERROR", Message: "failure", CreatedAt: time.Now().UTC().Add(-time.Second)}
}

func TestStatsCarryKeyAndSourceIDAndRequireAMatchingAcknowledgement(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ack   func(*ingestv1.IngestStatsRequest) *ingestv1.IngestStatsResponse
		valid bool
	}{
		{"valid", func(r *ingestv1.IngestStatsRequest) *ingestv1.IngestStatsResponse { return requeststest.AckStats(r, 7) }, true},
		{"empty", func(*ingestv1.IngestStatsRequest) *ingestv1.IngestStatsResponse {
			return &ingestv1.IngestStatsResponse{}
		}, false},
		{"wrong source", func(*ingestv1.IngestStatsRequest) *ingestv1.IngestStatsResponse {
			return &ingestv1.IngestStatsResponse{Acknowledgements: []*ingestv1.StatAcknowledgement{{Id: 9, HostId: 7, SourceId: 43}}}
		}, false},
		{"missing central ID", func(*ingestv1.IngestStatsRequest) *ingestv1.IngestStatsResponse {
			return &ingestv1.IngestStatsResponse{Acknowledgements: []*ingestv1.StatAcknowledgement{{HostId: 7, SourceId: 42}}}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			central := &requeststest.Central{Stats: func(r *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
				if ids := requeststest.StatIDs(r); len(ids) != 1 || ids[0] != 42 {
					t.Errorf("persistent numeric source ID missing: %v", ids)
				}
				return tc.ack(r), nil
			}}
			err := requeststest.Start(t, central, "test-key").SendMachineStats(context.Background(), []models.MachineStat{validStat(42)})
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t err=%v", tc.valid, err)
			}
			if keys := central.Keys(); len(keys) != 1 || keys[0] != "test-key" {
				t.Fatalf("host key not sent once: %v", keys)
			}
		})
	}
}

func TestErrorAcknowledgementMustAccountForEverySourceID(t *testing.T) {
	for _, tc := range []struct {
		resp  *ingestv1.IngestErrorsResponse
		valid bool
	}{
		{&ingestv1.IngestErrorsResponse{Accepted: 1, Inserted: 1, SourceIds: []int64{12}}, true},
		{&ingestv1.IngestErrorsResponse{Accepted: 1, Duplicates: 1, SourceIds: []int64{12}}, true},
		{&ingestv1.IngestErrorsResponse{}, false},
		{&ingestv1.IngestErrorsResponse{Accepted: 1}, false},
		{&ingestv1.IngestErrorsResponse{Accepted: 1, Inserted: 1, SourceIds: []int64{13}}, false},
	} {
		central := &requeststest.Central{Errors: func(*ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) { return tc.resp, nil }}
		err := requeststest.Start(t, central, "k").SendErrorLogs(context.Background(), []models.ErrorLog{validError(12)})
		if (err == nil) != tc.valid {
			t.Fatalf("response %v: unexpected result %v", tc.resp, err)
		}
	}
}

func TestLevelIsSentAsTheContractEnum(t *testing.T) {
	var got ingestv1.LogLevel
	central := &requeststest.Central{Errors: func(r *ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
		got = r.GetErrors()[0].GetLevel()
		return requeststest.AckErrors(r, 1), nil
	}}
	entry := validError(1)
	entry.Level = " critical " // accepted by local validation; normalized on the wire
	if err := requeststest.Start(t, central, "k").SendErrorLogs(context.Background(), []models.ErrorLog{entry}); err != nil {
		t.Fatal(err)
	}
	if got != ingestv1.LogLevel_LOG_LEVEL_CRITICAL {
		t.Fatalf("level sent as %v", got)
	}
}

func TestFullBatchesUseOneRequestAndRetainNumericIDs(t *testing.T) {
	var stats []models.MachineStat
	var logs []models.ErrorLog
	for i := int64(0); i < 100; i++ {
		id := int64(1<<63-1) - i
		stats = append(stats, validStat(id))
		logs = append(logs, validError(id))
	}
	central := &requeststest.Central{
		Stats: func(r *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
			for i, id := range requeststest.StatIDs(r) {
				if id != stats[i].ID {
					t.Error("source ID or order changed")
				}
			}
			return requeststest.AckStats(r, 1<<63-1), nil
		},
		Errors: func(r *ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
			for i, id := range requeststest.ErrorIDs(r) {
				if id != logs[i].ID {
					t.Error("source ID or order changed")
				}
			}
			return requeststest.AckErrors(r, 50), nil
		},
	}
	client := requeststest.Start(t, central, "k")
	if err := client.SendMachineStats(context.Background(), stats); err != nil {
		t.Fatal(err)
	}
	if err := client.SendErrorLogs(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	if central.Calls() != 2 {
		t.Fatalf("expected one request per 100-record batch, got %d", central.Calls())
	}
}

func TestBatchAcknowledgementCannotRepeatOrOmitMembers(t *testing.T) {
	ack := func(ids ...[2]int64) *ingestv1.IngestStatsResponse { // {central id, source id}, host 7
		resp := &ingestv1.IngestStatsResponse{}
		for _, id := range ids {
			resp.Acknowledgements = append(resp.Acknowledgements, &ingestv1.StatAcknowledgement{Id: id[0], HostId: 7, SourceId: id[1]})
		}
		return resp
	}
	mixedHosts := ack([2]int64{9, 2}, [2]int64{10, 1})
	mixedHosts.Acknowledgements[1].HostId = 8
	for name, resp := range map[string]*ingestv1.IngestStatsResponse{
		"omitted":         ack([2]int64{9, 2}),
		"repeated source": ack([2]int64{9, 2}, [2]int64{10, 2}),
		"repeated id":     ack([2]int64{9, 2}, [2]int64{9, 1}),
		"two hosts":       mixedHosts,
	} {
		central := &requeststest.Central{Stats: func(*ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) { return resp, nil }}
		if err := requeststest.Start(t, central, "k").SendMachineStats(context.Background(), []models.MachineStat{validStat(2), validStat(1)}); err == nil {
			t.Fatalf("%s: inconsistent acknowledgement accepted", name)
		}
	}
	for name, resp := range map[string]*ingestv1.IngestErrorsResponse{
		"omitted":  {Accepted: 2, Inserted: 2, SourceIds: []int64{2}},
		"repeated": {Accepted: 2, Inserted: 2, SourceIds: []int64{2, 2}},
		"foreign":  {Accepted: 2, Inserted: 2, SourceIds: []int64{2, 3}},
	} {
		central := &requeststest.Central{Errors: func(*ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) { return resp, nil }}
		if err := requeststest.Start(t, central, "k").SendErrorLogs(context.Background(), []models.ErrorLog{validError(2), validError(1)}); err == nil {
			t.Fatalf("%s: inconsistent acknowledgement accepted", name)
		}
	}
}

func TestFailureClassificationUsesOnlyContractCodes(t *testing.T) {
	for _, tc := range []struct {
		name          string
		err           error
		record, pause bool
		split         bool
		retryAfter    time.Duration
	}{
		{"record rejected", requeststest.Reject(42, ingestv1.RejectionReason_REJECTION_REASON_FUTURE_TIMESTAMP), true, false, false, 0},
		{"rejection without source", requeststest.Reject(0, ingestv1.RejectionReason_REJECTION_REASON_FUTURE_TIMESTAMP), false, true, false, 0},
		{"malformed batch", status.Error(codes.InvalidArgument, "duplicate source ID in batch"), false, true, false, 0},
		{"credential rejected", status.Error(codes.Unauthenticated, ""), false, true, false, 0},
		{"access denied", status.Error(codes.PermissionDenied, ""), false, true, false, 0},
		{"wrong service", status.Error(codes.Unimplemented, ""), false, true, false, 0},
		{"temporary failure", status.Error(codes.Unavailable, ""), false, false, false, 0},
		{"internal failure", status.Error(codes.Internal, ""), false, false, false, 0},
		{"overloaded", requeststest.RetryLater(codes.ResourceExhausted, time.Minute), false, false, false, time.Minute},
		{"unavailable with hint", requeststest.RetryLater(codes.Unavailable, 30*time.Second), false, false, false, 30 * time.Second},
		{"message too large", status.Error(codes.ResourceExhausted, "grpc: received message larger than max"), false, false, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			central := &requeststest.Central{Stats: func(*ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) { return nil, tc.err }}
			err := requeststest.Start(t, central, "k").SendMachineStats(context.Background(), []models.MachineStat{validStat(42)})
			var record *validation.RecordError
			if errors.As(err, &record) != tc.record {
				t.Fatalf("record classification: %v", err)
			}
			if tc.record {
				if record.SourceID != 42 || record.Reason != "central rejected record: future_timestamp" || strings.Contains(record.Reason, "private") {
					t.Fatalf("reason must come from the code only: %+v", record)
				}
				return
			}
			if errors.Is(err, requests.ErrBatchBodyTooLarge) != tc.split {
				t.Fatalf("split classification: %v", err)
			}
			if tc.split {
				return
			}
			var delivery *requests.DeliveryError
			if !errors.As(err, &delivery) || delivery.Pause != tc.pause || delivery.RetryAfter != tc.retryAfter {
				t.Fatalf("delivery classification: %+v (%v)", delivery, err)
			}
		})
	}
}

func TestOversizedBatchIsDetectedBeforeSending(t *testing.T) {
	central := &requeststest.Central{}
	entry := validError(1)
	entry.Message = strings.Repeat("m", 16384)
	stack := strings.Repeat("s", 65536)
	entry.StackTrace = &stack
	logs := make([]models.ErrorLog, 0, 20)
	for i := int64(1); i <= 20; i++ {
		e := entry
		e.ID = i
		logs = append(logs, e)
	}
	err := requeststest.Start(t, central, "k").SendErrorLogs(context.Background(), logs)
	if !errors.Is(err, requests.ErrBatchBodyTooLarge) || central.Calls() != 0 {
		t.Fatalf("want local size rejection without a request, got %v after %d calls", err, central.Calls())
	}
}

func TestClientRejectsUnsafeAddresses(t *testing.T) {
	for _, address := range []string{
		"http://remote.example:3334", "https://user:pass@central.example", "https://central.example?x=y",
		"https://central.example#fragment", "https://central.example/ingest", "ftp://central.example", "central.example:3334",
	} {
		if _, err := requests.NewClient(address, "key"); err == nil {
			t.Fatalf("unsafe address accepted: %s", address)
		}
	}
	for _, address := range []string{"https://central.example", "https://central.example:8443", "http://localhost:3334", "http://127.0.0.1:3334"} {
		client, err := requests.NewClient(address, "key")
		if err != nil {
			t.Fatalf("valid address rejected: %s: %v", address, err)
		}
		_ = client.Close()
	}
	if _, err := requests.NewClient("https://central.example", " "); err == nil {
		t.Fatal("empty host key accepted")
	}
}

func TestStatisticsCarryTheBufferSnapshot(t *testing.T) {
	var got *ingestv1.BufferStatus
	central := &requeststest.Central{Stats: func(r *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
		got = r.GetBuffer()
		return requeststest.AckStats(r, 7), nil
	}}
	client := requeststest.Start(t, central, "k")
	observed := time.Now().Add(-time.Second)
	status := &models.BufferStatus{ObservedAt: observed, Pending: 4, Quarantined: 3, OldestPendingAgeSeconds: 60,
		DeliveryPaused: true, SizeBytes: 1024, DroppedByKind: map[string]int64{"INFO": 9}}
	if err := client.SendMachineStatsWithBuffer(context.Background(), []models.MachineStat{validStat(42)}, status); err != nil {
		t.Fatal(err)
	}
	if got == nil || !got.GetObservedAt().AsTime().Equal(observed.UTC()) || got.GetPending() != 4 || got.GetQuarantined() != 3 ||
		got.GetOldestPendingAgeSeconds() != 60 || !got.GetDeliveryPaused() || got.GetSizeBytes() != 1024 || got.GetDroppedByKind()["INFO"] != 9 {
		t.Fatalf("buffer snapshot %v", got)
	}
	if err := client.SendMachineStats(context.Background(), []models.MachineStat{validStat(43)}); err != nil || got != nil {
		t.Fatalf("statistics without a snapshot must not carry one: %v %v", got, err)
	}
}

// ADR-0011: SIGHUP swaps the key used by the next requests.
func TestAPIKeyCanBeReplaced(t *testing.T) {
	central := &requeststest.Central{}
	client := requeststest.Start(t, central, "old-key")
	if err := client.SetAPIKey(" "); err == nil {
		t.Fatal("an empty key must be refused")
	}
	if err := client.SetAPIKey(" new-key "); err != nil {
		t.Fatal(err)
	}
	if err := client.SendMachineStats(context.Background(), []models.MachineStat{validStat(42)}); err != nil {
		t.Fatal(err)
	}
	if keys := central.Keys(); len(keys) != 1 || keys[0] != "new-key" {
		t.Fatalf("keys sent: %v", keys)
	}
}
