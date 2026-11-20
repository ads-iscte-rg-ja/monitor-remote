// Package bufferlimit runs the buffer limit of ADR-0009 on its own schedule:
// cleanup is retention (hourly), the limit protects the host (seconds).
package bufferlimit

import (
	"context"
	"time"

	"monitor-remote/internal/repository"
)

// Enforcer is the buffer operation the worker needs.
type Enforcer interface {
	EnforceBufferLimit(ctx context.Context, limits repository.BufferLimits, now time.Time) (repository.BufferEnforcement, error)
}

// Run enforces the limits at start (the buffer may already be over them) and
// then after each interval, until ctx ends.
func Run(ctx context.Context, enforcer Enforcer, limits repository.BufferLimits, interval time.Duration, logf func(string, ...any)) {
	for ctx.Err() == nil {
		check(ctx, enforcer, limits, logf)
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func check(ctx context.Context, enforcer Enforcer, limits repository.BufferLimits, logf func(string, ...any)) {
	checkCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	result, err := enforcer.EnforceBufferLimit(checkCtx, limits, time.Now().UTC())
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		logf("buffer limit: %v", err)
		return
	}
	if result.HardLimit {
		logf("buffer limit: CRITICAL hard limit reached; size=%d->%d bytes; delivered_removed=%d; dropped %s",
			result.SizeBefore, result.SizeAfter, result.Delivered, repository.FormatDrops(result.Dropped))
	} else if len(result.Dropped) > 0 || result.Delivered > 0 {
		logf("buffer limit: size=%d->%d bytes; delivered_removed=%d; dropped %s",
			result.SizeBefore, result.SizeAfter, result.Delivered, repository.FormatDrops(result.Dropped))
	}
}
