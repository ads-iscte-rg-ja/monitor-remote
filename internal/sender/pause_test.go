package sender

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	ingestv1 "github.com/ads-iscte-rg-ja/monitor-api/gen/go/monitor/ingest/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"monitor-remote/internal/requests"
	"monitor-remote/internal/requests/requeststest"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// switchableCentral refuses the key until accept is set, then acknowledges, and
// records the size of each batch it received.
type switchableCentral struct {
	*requeststest.Central
	accept atomic.Bool
	mu     sync.Mutex
	sizes  []int
	paused []bool
}

func newSwitchableCentral(refusal codes.Code) *switchableCentral {
	c := &switchableCentral{}
	c.Central = &requeststest.Central{
		Errors: func(r *ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
			c.note(len(r.GetErrors()), false)
			if !c.accept.Load() {
				return nil, status.Error(refusal, "refused")
			}
			return requeststest.AckErrors(r, len(r.GetErrors())), nil
		},
		Stats: func(r *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
			c.note(len(r.GetStats()), r.GetBuffer().GetDeliveryPaused())
			if !c.accept.Load() {
				return nil, status.Error(refusal, "refused")
			}
			return requeststest.AckStats(r, 7), nil
		},
	}
	return c
}

func (c *switchableCentral) note(size int, paused bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sizes = append(c.sizes, size)
	c.paused = append(c.paused, paused)
}

func (c *switchableCentral) last() (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sizes[len(c.sizes)-1], c.paused[len(c.paused)-1]
}

// AV3: a 4xx-like refusal pauses delivery; nothing is sent for 5 minutes; then
// one probe of one record; central accepts it now, and delivery resumes in the
// same cycle without restarting the agent. The clock is injected: no sleeps.
func TestPausedDeliveryResumesWithinFiveMinutes(t *testing.T) {
	ctx := context.Background()
	repo, count := openRepo(t, "av3.db")
	for i := 0; i < 3; i++ {
		saveError(t, repo, nil)
	}
	saveStat(t, repo)
	saveStat(t, repo)
	central := newSwitchableCentral(codes.PermissionDenied)
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	s := New(repo, requeststest.Start(t, central.Central, "key"))
	s.now = clock.Now
	var logs []string
	s.SetLogger(func(format string, _ ...any) { logs = append(logs, format) })

	var paused *requests.DeliveryError
	if err := s.SendPendingErrorLogs(ctx); !errors.As(err, &paused) || !paused.Pause {
		t.Fatalf("refusal must pause delivery: %v", err)
	}
	clock.Advance(5*time.Minute - time.Second)
	for _, send := range []func(context.Context) error{s.SendPendingErrorLogs, s.SendPendingMachineStats} {
		if err := send(ctx); !errors.Is(err, errPaused) {
			t.Fatalf("before the probe: %v", err)
		}
	}
	if central.Calls() != 1 {
		t.Fatalf("a paused delivery sent %d requests before the probe", central.Calls()-1)
	}

	// The operator fixed the cause; the probe finds it at the 5-minute mark.
	central.accept.Store(true)
	clock.Advance(time.Second)
	if err := s.SendPendingMachineStats(ctx); err != nil {
		t.Fatal(err)
	}
	// The probe reports the pause (MN1, ADR-0010); the batch after the resume does not.
	if size, reportedPaused := central.last(); central.Calls() != 3 || central.sizes[1] != 1 || !central.paused[1] || size != 1 || reportedPaused {
		t.Fatalf("want a probe of 1 record reporting the pause, then the rest: sizes=%v paused=%v", central.sizes, central.paused)
	}
	if err := s.SendPendingErrorLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if size, _ := central.last(); size != 3 {
		t.Fatalf("after resuming, errors must go in a full batch: %v", central.sizes)
	}
	if n := count("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL"); n != 0 {
		t.Fatalf("%d records still pending after resuming", n)
	}
	if len(logs) != 2 {
		t.Fatalf("pause and resume must be logged: %q", logs)
	}
}

func TestFailedProbeKeepsThePauseAndBooksTheNextOne(t *testing.T) {
	ctx := context.Background()
	repo, _ := openRepo(t, "probe-fails.db")
	saveError(t, repo, nil)
	saveError(t, repo, nil)
	central := newSwitchableCentral(codes.Unauthenticated)
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	s := New(repo, requeststest.Start(t, central.Central, "key"))
	s.now = clock.Now

	_ = s.SendPendingErrorLogs(ctx)
	clock.Advance(5 * time.Minute)
	if err := s.SendPendingErrorLogs(ctx); err == nil || errors.Is(err, errPaused) {
		t.Fatalf("the probe must reach central and fail: %v", err)
	}
	if central.Calls() != 2 || central.sizes[1] != 1 {
		t.Fatalf("probe: %v", central.sizes)
	}
	clock.Advance(5*time.Minute - time.Second)
	if err := s.SendPendingErrorLogs(ctx); !errors.Is(err, errPaused) || central.Calls() != 2 {
		t.Fatalf("a failed probe must book the next one 5 minutes later: %v", err)
	}
	clock.Advance(time.Second)
	_ = s.SendPendingErrorLogs(ctx)
	if central.Calls() != 3 {
		t.Fatal("the next probe did not run")
	}
}

// A probe that central validates and rejects proves the key good: it resumes.
func TestProbeRejectedAsARecordResumesDelivery(t *testing.T) {
	ctx := context.Background()
	repo, count := openRepo(t, "probe-reject.db")
	saveError(t, repo, nil)
	saveError(t, repo, nil)
	calls := 0
	central := &requeststest.Central{Errors: func(r *ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
		calls++
		switch calls {
		case 1:
			return nil, status.Error(codes.Unauthenticated, "revoked")
		case 2:
			return nil, requeststest.Reject(requeststest.ErrorIDs(r)[0], ingestv1.RejectionReason_REJECTION_REASON_FUTURE_TIMESTAMP)
		default:
			return requeststest.AckErrors(r, len(r.GetErrors())), nil
		}
	}}
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	s := New(repo, requeststest.Start(t, central, "key"))
	s.now = clock.Now
	_ = s.SendPendingErrorLogs(ctx)
	clock.Advance(5 * time.Minute)
	if err := s.SendPendingErrorLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || count("SELECT COUNT(*) FROM delivery_status WHERE quarantine_reason IS NOT NULL") != 1 ||
		count("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NOT NULL") != 1 {
		t.Fatalf("calls=%d: the rejected probe record goes to quarantine and delivery resumes", calls)
	}
}

// A category with nothing to send gives the probe back to the other one.
func TestUnusedProbeIsGivenBack(t *testing.T) {
	ctx := context.Background()
	repo, _ := openRepo(t, "probe-unused.db")
	stat := saveStat(t, repo)
	central := newSwitchableCentral(codes.Unauthenticated)
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	s := New(repo, requeststest.Start(t, central.Central, "key"))
	s.now = clock.Now
	_ = s.SendPendingMachineStats(ctx)
	central.accept.Store(true)
	clock.Advance(5 * time.Minute)
	if err := s.SendPendingErrorLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.SendPendingMachineStats(ctx); err != nil {
		t.Fatalf("the statistics must probe after the errors gave the probe back: %v", err)
	}
	if pending, err := repo.ListPendingMachineStats(ctx, 10); err != nil || len(pending) != 0 || central.Calls() != 2 {
		t.Fatalf("statistic %d not delivered by the probe: %v %v", stat.ID, pending, err)
	}
}

func TestOnlyOneProbeAtATime(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	s := &Sender{now: clock.Now, batchLimit: 100}
	s.observeFailure(&requests.DeliveryError{Code: codes.Unauthenticated, Pause: true})
	clock.Advance(DefaultProbeInterval)
	first, err := s.admit()
	if err != nil || !first.probe || first.limit != 1 {
		t.Fatalf("first admission must be a probe of one record: %+v %v", first, err)
	}
	if _, err := s.admit(); !errors.Is(err, errPaused) {
		t.Fatal("a second probe ran while the first was in flight")
	}
	s.endStep(first, false)
	if _, err := s.admit(); !errors.Is(err, errPaused) {
		t.Fatal("the next probe must wait for its booked time")
	}
	s.endStep(admission{limit: 100}, false) // a normal step has nothing to close
	s.resume()
	s.resume() // idempotent
	if step, err := s.admit(); err != nil || step.probe || step.limit != 100 {
		t.Fatalf("after resuming, a full batch: %+v %v", step, err)
	}
}

// SIGHUP: the reloaded key is used at once by an immediate probe.
func TestReloadedCredentialProbesAtOnce(t *testing.T) {
	ctx := context.Background()
	repo, _ := openRepo(t, "reload.db")
	saveError(t, repo, nil)
	central := newSwitchableCentral(codes.Unauthenticated)
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	s := New(repo, requeststest.Start(t, central.Central, "old-key"))
	s.now = clock.Now
	_ = s.SendPendingErrorLogs(ctx)

	if err := s.ReloadCredential(func() (string, error) { return "", errors.New(".env unreadable") }); err == nil {
		t.Fatal("a failed reload must be reported")
	}
	if err := s.ReloadCredential(func() (string, error) { return "  ", nil }); err == nil {
		t.Fatal("an empty key must be refused")
	}
	central.accept.Store(true)
	if err := s.ReloadCredential(func() (string, error) { return "new-key", nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.SendPendingErrorLogs(ctx); err != nil {
		t.Fatalf("the reload must probe at once, without waiting 5 minutes: %v", err)
	}
	if keys := central.Keys(); len(keys) != 2 || keys[0] != "old-key" || keys[1] != "new-key" {
		t.Fatalf("keys sent: %v", keys)
	}
}

// The workers survive a pause: the run loop sleeps until the probe and goes on
// delivering after it (real clock, short probe interval).
func TestRunLoopSurvivesAPause(t *testing.T) {
	repo, count := openRepo(t, "run-pause.db")
	saveError(t, repo, nil)
	central := newSwitchableCentral(codes.Unauthenticated)
	s := New(repo, requeststest.Start(t, central.Central, "key"))
	s.probeInterval = 50 * time.Millisecond
	// The resume is logged after the probe's record is marked sent: waiting for
	// it needs no polling and no sleep (TE3, ADR-0014).
	resumed := make(chan struct{}, 1)
	s.SetLogger(func(format string, _ ...any) {
		if strings.HasPrefix(format, "delivery resumed") {
			resumed <- struct{}{}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RunErrorLogs(ctx, 10*time.Millisecond, func(error) { central.accept.Store(true) })
	}()
	select {
	case <-resumed:
	case <-ctx.Done():
		t.Fatal("delivery did not resume after the pause")
	}
	cancel()
	<-done
	if n := count("SELECT COUNT(*) FROM delivery_status WHERE sent_at IS NULL"); n != 0 {
		t.Fatalf("%d records still pending after the resume", n)
	}
}

func TestReloadOnSignal(t *testing.T) {
	repo, _ := openRepo(t, "signal.db")
	central := &requeststest.Central{}
	s := New(repo, requeststest.Start(t, central, "old-key"))
	var mu sync.Mutex
	var logs []string
	s.SetLogger(func(format string, _ ...any) { mu.Lock(); logs = append(logs, format); mu.Unlock() })
	signals := make(chan os.Signal)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	keys := []string{"", "new-key", "new-key"}
	go func() {
		defer close(done)
		s.ReloadOnSignal(ctx, signals, func() (string, error) {
			key := keys[0]
			keys = keys[1:]
			if key == "" {
				return "", errors.New("HOST_API_KEY is missing")
			}
			return key, nil
		})
	}()
	signals <- syscall.SIGHUP
	signals <- syscall.SIGHUP
	signals <- syscall.SIGHUP // only delivered once the second reload finished
	cancel()
	<-done
	saveError(t, repo, nil)
	if err := s.SendPendingErrorLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := central.Keys(); len(got) != 1 || got[0] != "new-key" || len(logs) < 2 {
		t.Fatalf("keys %v, logs %q", got, logs)
	}
}
