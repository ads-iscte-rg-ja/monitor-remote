package sender

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"monitor-remote/internal/requests"
)

func (s *Sender) RunMachineStats(ctx context.Context, interval time.Duration, report func(error)) {
	s.run(ctx, interval, s.SendPendingMachineStats, report)
}

func (s *Sender) RunErrorLogs(ctx context.Context, interval time.Duration, report func(error)) {
	s.run(ctx, interval, s.SendPendingErrorLogs, report)
}

// Retry state is per delivery category. A pause or Retry-After deadline is shared
// because both categories authenticate with the same host credential.
func (s *Sender) run(ctx context.Context, interval time.Duration, send func(context.Context) error, report func(error)) {
	delay, failures := time.Duration(0), 0
	for s.wait(ctx, delay) {
		err := send(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			failures, delay = 0, interval
			continue
		}
		if report != nil {
			report(err)
		}
		failures = min(failures+1, 7)
		delay = retryDelay(failures)
		var deliveryErr *requests.DeliveryError
		if errors.As(err, &deliveryErr) && deliveryErr.RetryAfter > delay {
			delay = deliveryErr.RetryAfter
		}
	}
}

func retryDelay(failures int) time.Duration {
	// 5s, 10s, ... capped at 5m, with bounded jitter. This random value is only
	// scheduling jitter, not credential generation.
	base := min(5*time.Second*time.Duration(1<<min(max(failures-1, 0), 6)), 5*time.Minute)
	return min(base*4/5+time.Duration(rand.Int64N(int64(base*2/5)+1)), 5*time.Minute)
}

func (s *Sender) wait(ctx context.Context, delay time.Duration) bool {
	if delay > 0 && !waitDuration(ctx, delay) {
		return false
	}
	for ctx.Err() == nil {
		s.mu.Lock()
		paused, notBefore := s.paused, s.notBefore
		s.mu.Unlock()
		if paused {
			return false
		}
		remaining := time.Until(notBefore)
		if remaining <= 0 {
			return true
		}
		if !waitDuration(ctx, remaining) {
			return false
		}
	}
	return false
}

func waitDuration(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
