package sender

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"

	"monitor-remote/internal/models"
	"monitor-remote/internal/repository"
	"monitor-remote/internal/requests"
	"monitor-remote/internal/validation"
)

type Sender struct {
	repository *repository.QueueRepository
	client     *requests.Client
	batchLimit int
	mu         sync.Mutex
	paused     bool
	notBefore  time.Time
}

func New(repository *repository.QueueRepository, client *requests.Client) *Sender {
	return &Sender{repository: repository, client: client, batchLimit: requests.MaxBatchRecords}
}

// Each cycle flushes the finite backlog accepted at its start, in newest-first
// batches. A delivery failure stops immediately and leaves remaining rows pending.
func (s *Sender) SendPendingMachineStats(ctx context.Context) error {
	cutoff, err := s.repository.DeliveryCutoff(ctx)
	if err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		stats, err := s.repository.ListPendingMachineStatsThrough(ctx, s.batchLimit, cutoff)
		if err != nil {
			return err
		}
		if len(stats) == 0 {
			return nil
		}
		batch := make([]pendingDelivery[models.MachineStat], 0, len(stats))
		for _, pending := range stats {
			invalid := validation.MachineStat(pending.Stat, time.Now().UTC())
			if pending.InvalidReason != "" {
				invalid = validation.Invalid(pending.InvalidReason)
			}
			if invalid != nil {
				if err := s.quarantine(ctx, pending.DeliveryID, invalid); err != nil {
					return err
				}
				continue
			}
			batch = append(batch, pendingDelivery[models.MachineStat]{deliveryID: pending.DeliveryID, sourceID: pending.Stat.ID, record: pending.Stat})
		}
		if err := deliverBatch(ctx, s, batch, s.client.SendMachineStats); err != nil {
			return err
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
		logs, err := s.repository.ListPendingErrorLogsThrough(ctx, s.batchLimit, cutoff)
		if err != nil {
			return err
		}
		if len(logs) == 0 {
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
		if err := deliverBatch(ctx, s, batch, s.client.SendErrorLogs); err != nil {
			return err
		}
	}
}

func (s *Sender) quarantine(ctx context.Context, id int64, err error) error {
	var recordErr *validation.RecordError
	if !errors.As(err, &recordErr) {
		return err
	}
	return s.repository.Quarantine(ctx, id, recordErr.Reason)
}

func (s *Sender) observeFailure(err error) {
	var deliveryErr *requests.DeliveryError
	if !errors.As(err, &deliveryErr) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if deliveryErr.Pause {
		s.paused = true
	}
	if deliveryErr.RetryAfter > 0 {
		next := time.Now().Add(deliveryErr.RetryAfter)
		if next.After(s.notBefore) {
			s.notBefore = next
		}
	}
}

var errPaused = errors.New("central delivery paused until configuration is corrected and remote restarts")

func (s *Sender) deliveryGate() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paused {
		return errPaused
	}
	if delay := time.Until(s.notBefore); delay > 0 {
		return &requests.DeliveryError{Code: codes.ResourceExhausted, RetryAfter: delay}
	}
	return nil
}
