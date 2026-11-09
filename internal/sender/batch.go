package sender

import (
	"context"
	"errors"
	"fmt"
	"time"

	"monitor-remote/internal/requests"
	"monitor-remote/internal/validation"
)

type pendingDelivery[T any] struct {
	deliveryID int64
	sourceID   int64
	record     T
}

func deliverBatch[T any](ctx context.Context, s *Sender, batch []pendingDelivery[T], send func(context.Context, []T) error) error {
	for len(batch) > 0 {
		if err := s.deliveryGate(); err != nil {
			return err
		}
		records := make([]T, 0, len(batch))
		ids := make([]int64, 0, len(batch))
		for _, pending := range batch {
			records = append(records, pending.record)
			ids = append(ids, pending.deliveryID)
		}
		if err := s.repository.MarkAttempts(ctx, ids, time.Now().UTC()); err != nil {
			return err
		}
		err := send(ctx, records)
		if err == nil {
			return s.repository.MarkSentBatch(ctx, ids, time.Now().UTC())
		}
		if errors.Is(err, requests.ErrBatchBodyTooLarge) {
			if len(batch) == 1 {
				return s.repository.Quarantine(ctx, batch[0].deliveryID, "record exceeds central's request body size limit")
			}
			// Splitting retains newest-first order and never quarantines an entire
			// valid batch merely because its combined encoding is too large.
			mid := len(batch) / 2
			if err := deliverBatch(ctx, s, batch[:mid], send); err != nil {
				return err
			}
			return deliverBatch(ctx, s, batch[mid:], send)
		}
		var rejected *validation.RecordError
		if errors.As(err, &rejected) {
			index := -1
			for i, pending := range batch {
				if pending.sourceID == rejected.SourceID {
					index = i
					break
				}
			}
			if index < 0 {
				contractErr := &requests.DeliveryError{StatusCode: 400, Pause: true}
				s.observeFailure(contractErr)
				return fmt.Errorf("central rejection did not identify a submitted source ID: %w", contractErr)
			}
			if err := s.repository.Quarantine(ctx, batch[index].deliveryID, rejected.Reason); err != nil {
				return err
			}
			// Central validates the entire batch before writing it. Retry the
			// remaining members together after excluding the identified bad row.
			batch = append(batch[:index:index], batch[index+1:]...)
			continue
		}
		s.observeFailure(err)
		return err
	}
	return nil
}
