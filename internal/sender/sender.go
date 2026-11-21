package sender

import (
	"context"
	"errors"
	"sync"
	"time"

	"monitor-remote/internal/models"
	"monitor-remote/internal/repository"
	"monitor-remote/internal/requests"
	"monitor-remote/internal/validation"
)

type Sender struct {
	repository *repository.QueueRepository
	client     *requests.Client
	batchLimit int

	// Delivery state shared by both categories (ADR-0011). now and
	// probeInterval are injectable for tests; zero values mean time.Now and
	// DefaultProbeInterval.
	mu            sync.Mutex
	now           func() time.Time
	probeInterval time.Duration
	paused        bool
	probing       bool
	probeAt       time.Time
	notBefore     time.Time
	wake          chan struct{}
	logf          func(string, ...any)

	// statsQueued holds one token when a statistic was stored since the
	// statistics worker last looked (NotifyStatsQueued).
	statsQueued chan struct{}
}

func New(repository *repository.QueueRepository, client *requests.Client) *Sender {
	return &Sender{repository: repository, client: client, batchLimit: requests.MaxBatchRecords, statsQueued: make(chan struct{}, 1)}
}

// NotifyStatsQueued tells the statistics worker that a statistic was stored. It
// never blocks; a worker that found nothing to send is waiting for it.
func (s *Sender) NotifyStatsQueued() {
	select {
	case s.statsQueued <- struct{}{}:
	default:
	}
}

// deliverMachineStats is one cycle of the statistics worker. The buffer snapshot
// travels only with statistics (ADR-0010), so a cycle that finds none would leave
// central one cycle behind: collection and delivery run on independent timers of
// the same length, and delivery often wakes just before collection (always at
// start-up). Such a cycle waits, at most one interval, for the next statistic and
// sends it with a fresh snapshot (ADR-0012, found by the MN1 end-to-end test).
func (s *Sender) deliverMachineStats(ctx context.Context, interval time.Duration) error {
	select {
	case <-s.statsQueued: // already pending, this cycle sends it
	default:
	}
	pending, err := s.sendPendingMachineStats(ctx)
	if err != nil || pending {
		return err
	}
	if !waitDuration(ctx, interval, s.statsQueued) {
		return ctx.Err()
	}
	_, err = s.sendPendingMachineStats(ctx)
	return err
}

// Each cycle flushes the finite backlog accepted at its start, in newest-first
// batches. A delivery failure stops immediately and leaves remaining rows pending.
func (s *Sender) SendPendingMachineStats(ctx context.Context) error {
	_, err := s.sendPendingMachineStats(ctx)
	return err
}

// sendPendingMachineStats also reports whether any statistic was found pending.
func (s *Sender) sendPendingMachineStats(ctx context.Context) (bool, error) {
	cutoff, err := s.repository.DeliveryCutoff(ctx)
	if err != nil {
		return false, err
	}
	send := s.statsSender(ctx)
	for found := false; ; found = true {
		if err := ctx.Err(); err != nil {
			return found, err
		}
		step, err := s.admit()
		if err != nil {
			return found, err
		}
		stats, err := s.repository.ListPendingMachineStatsThrough(ctx, step.limit, cutoff)
		if err != nil {
			s.endStep(step, true)
			return found, err
		}
		if len(stats) == 0 {
			s.endStep(step, true)
			return found, nil
		}
		batch := make([]pendingDelivery[models.MachineStat], 0, len(stats))
		for _, pending := range stats {
			invalid := validation.MachineStat(pending.Stat, time.Now().UTC())
			if pending.InvalidReason != "" {
				invalid = validation.Invalid(pending.InvalidReason)
			}
			if invalid != nil {
				if err := s.quarantine(ctx, pending.DeliveryID, invalid); err != nil {
					return true, err
				}
				continue
			}
			batch = append(batch, pendingDelivery[models.MachineStat]{deliveryID: pending.DeliveryID, sourceID: pending.Stat.ID, record: pending.Stat})
		}
		if err := deliverStep(ctx, s, step, batch, send); err != nil {
			return true, err
		}
	}
}

func (s *Sender) SendPendingErrorLogs(ctx context.Context) error {
	cutoff, err := s.repository.DeliveryCutoff(ctx)
	if err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		step, err := s.admit()
		if err != nil {
			return err
		}
		logs, err := s.repository.ListPendingErrorLogsThrough(ctx, step.limit, cutoff)
		if err != nil {
			s.endStep(step, true)
			return err
		}
		if len(logs) == 0 {
			s.endStep(step, true)
			return nil
		}
		batch := make([]pendingDelivery[models.ErrorLog], 0, len(logs))
		for _, pending := range logs {
			invalid := validation.ErrorLog(pending.ErrorLog, time.Now().UTC())
			if pending.InvalidReason != "" {
				invalid = validation.Invalid(pending.InvalidReason)
			}
			if invalid != nil {
				if err := s.quarantine(ctx, pending.DeliveryID, invalid); err != nil {
					return err
				}
				continue
			}
			batch = append(batch, pendingDelivery[models.ErrorLog]{deliveryID: pending.DeliveryID, sourceID: pending.ErrorLog.ID, record: pending.ErrorLog})
		}
		if err := deliverStep(ctx, s, step, batch, s.client.SendErrorLogs); err != nil {
			return err
		}
	}
}

// statsSender sends the cycle's statistics with one buffer snapshot, taken at
// the start of the cycle (ADR-0010). The snapshot is accessory: if it cannot be
// taken, the statistics are still delivered, without it.
func (s *Sender) statsSender(ctx context.Context) func(context.Context, []models.MachineStat) error {
	status, err := s.repository.BufferStatus(ctx, time.Now())
	if err != nil {
		return s.client.SendMachineStats
	}
	return func(ctx context.Context, stats []models.MachineStat) error {
		// The pause is read at each send: a probe reports it, the batches after
		// a resume in the same cycle do not (ADR-0011).
		batchStatus := status
		s.mu.Lock()
		batchStatus.DeliveryPaused = s.paused
		s.mu.Unlock()
		return s.client.SendMachineStatsWithBuffer(ctx, stats, &batchStatus)
	}
}

func (s *Sender) quarantine(ctx context.Context, id int64, err error) error {
	var recordErr *validation.RecordError
	if !errors.As(err, &recordErr) {
		return err
	}
	return s.repository.Quarantine(ctx, id, recordErr.Reason)
}

// deliverStep sends one admitted step. A probe whose only record was dropped
// locally (quarantined before sending) sent nothing, so it is given back.
func deliverStep[T any](ctx context.Context, s *Sender, step admission, batch []pendingDelivery[T], send func(context.Context, []T) error) error {
	if len(batch) == 0 {
		s.endStep(step, true)
		return nil
	}
	err := deliverBatch(ctx, s, batch, send, step.probe)
	s.endStep(step, false)
	return err
}

// endStep closes a probe that did not resume delivery: unused, it is given back
// at once; used, the next one stays booked.
func (s *Sender) endStep(step admission, unused bool) {
	if !step.probe {
		return
	}
	if unused {
		s.releaseProbe()
		return
	}
	s.finishProbe()
}
