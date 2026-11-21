package sender

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"

	"monitor-remote/internal/requests"
	"monitor-remote/internal/validation"
)

type pendingDelivery[T any] struct {
	deliveryID int64
	sourceID   int64
	record     T
}

// deliverBatch sends a batch, splitting it or dropping a rejected member as
// needed. A probe (ADR-0011) skips the pause gate, and resumes delivery when
// central proves the key and the contract good: it accepts the record, or
// validates and rejects it.
func deliverBatch[T any](ctx context.Context, s *Sender, batch []pendingDelivery[T], send func(context.Context, []T) error, probe bool) error {
	for len(batch) > 0 {
		if !probe {
			if err := s.deliveryGate(); err != nil {
				return err
			}
		}
		delivered, err := sendOnce(ctx, s, batch, send)
		if delivered {
			s.probeSucceeded(probe)
			return err
		}
		rest, retry, err := afterFailure(ctx, s, batch, send, probe, err)
		if !retry {
			return err
		}
		// Central validated the batch and named one bad record: the key and the
		// contract are good.
		s.probeSucceeded(probe)
		batch = rest
	}
	return nil
}

// afterFailure handles a batch central did not accept: split one too large,
// drop a rejected record and retry the rest, or record the failure.
func afterFailure[T any](ctx context.Context, s *Sender, batch []pendingDelivery[T], send func(context.Context, []T) error, probe bool, err error) ([]pendingDelivery[T], bool, error) {
	if errors.Is(err, requests.ErrBatchBodyTooLarge) {
		return nil, false, splitBatch(ctx, s, batch, send, probe)
	}
	var rejected *validation.RecordError
	if !errors.As(err, &rejected) {
		s.observeFailure(err)
		return nil, false, err
	}
	rest, err := dropRejected(ctx, s, batch, rejected)
	return rest, err == nil, err
}

// sendOnce marks the attempt, sends the batch and, if central acknowledged it,
// marks it sent. delivered reports that central accepted the batch.
func sendOnce[T any](ctx context.Context, s *Sender, batch []pendingDelivery[T], send func(context.Context, []T) error) (bool, error) {
	records := make([]T, 0, len(batch))
	ids := make([]int64, 0, len(batch))
	for _, pending := range batch {
		records = append(records, pending.record)
		ids = append(ids, pending.deliveryID)
	}
	if err := s.repository.MarkAttempts(ctx, ids, time.Now().UTC()); err != nil {
		return false, err
	}
	if err := send(ctx, records); err != nil {
		return false, err
	}
	return true, s.repository.MarkSentBatch(ctx, ids, time.Now().UTC())
}

// splitBatch retains newest-first order and never quarantines an entire valid
// batch merely because its combined encoding is too large.
func splitBatch[T any](ctx context.Context, s *Sender, batch []pendingDelivery[T], send func(context.Context, []T) error, probe bool) error {
	if len(batch) == 1 {
		return s.repository.Quarantine(ctx, batch[0].deliveryID, "record exceeds central's request body size limit")
	}
	mid := len(batch) / 2
	if err := deliverBatch(ctx, s, batch[:mid], send, probe); err != nil {
		return err
	}
	return deliverBatch(ctx, s, batch[mid:], send, probe)
}

// dropRejected quarantines the member central identified and returns the rest.
// Central validates the entire batch before writing it, so the remaining
// members are retried together. A rejection naming no submitted record is a
// contract violation: it pauses delivery.
func dropRejected[T any](ctx context.Context, s *Sender, batch []pendingDelivery[T], rejected *validation.RecordError) ([]pendingDelivery[T], error) {
	for i, pending := range batch {
		if pending.sourceID != rejected.SourceID {
			continue
		}
		if err := s.repository.Quarantine(ctx, pending.deliveryID, rejected.Reason); err != nil {
			return nil, err
		}
		return append(batch[:i:i], batch[i+1:]...), nil
	}
	contractErr := &requests.DeliveryError{Code: codes.InvalidArgument, Pause: true}
	s.observeFailure(contractErr)
	return nil, fmt.Errorf("central rejection did not identify a submitted source ID: %w", contractErr)
}
