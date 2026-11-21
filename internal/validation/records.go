// Package validation defines the remote-side record checks. A future local RPC
// can apply the same checks before committing a record.
package validation

import (
	"encoding/json"
	"math"
	"strings"
	"time"

	"monitor-remote/internal/models"
)

// RecordError is a record-specific failure. Its reason contains no payload or
// credentials and is safe to preserve with the quarantined delivery entry.
type RecordError struct {
	Reason   string
	SourceID int64
}

func (e *RecordError) Error() string { return e.Reason }
func Invalid(reason string) error    { return &RecordError{Reason: reason} }

func MachineStat(s models.MachineStat, now time.Time) error {
	if s.ID <= 0 {
		return Invalid("statistics source ID must be positive")
	}
	if !finite(s.CPUPercent) || s.CPUPercent < 0 || s.CPUPercent > 100 {
		return Invalid("CPU percent must be finite and between 0 and 100")
	}
	if !finite(s.RAMUsedMB) || !finite(s.RAMTotalMB) || s.RAMTotalMB <= 0 || s.RAMUsedMB < 0 || s.RAMUsedMB > s.RAMTotalMB {
		return Invalid("invalid RAM usage")
	}
	if !finite(s.DiskUsedGB) || !finite(s.DiskTotalGB) || s.DiskTotalGB <= 0 || s.DiskUsedGB < 0 || s.DiskUsedGB > s.DiskTotalGB {
		return Invalid("invalid disk usage")
	}
	return timestamp(s.CollectedAt, now)
}

func ErrorLog(e models.ErrorLog, now time.Time) error {
	if e.ID <= 0 {
		return Invalid("error source ID must be positive")
	}
	app, environment := strings.TrimSpace(e.AppName), strings.TrimSpace(e.Environment)
	if app == "" || environment == "" || strings.TrimSpace(e.Message) == "" {
		return Invalid("application, environment, and message are required")
	}
	if len(app) > 200 {
		return Invalid("application exceeds 200 bytes")
	}
	if len(environment) > 100 {
		return Invalid("environment exceeds 100 bytes")
	}
	if len(e.Message) > 16384 {
		return Invalid("message exceeds 16 KiB")
	}
	if e.StackTrace != nil && len(*e.StackTrace) > 65536 {
		return Invalid("stack trace exceeds 64 KiB")
	}
	if len(e.Metadata) > 32768 {
		return Invalid("metadata exceeds 32 KiB")
	}
	if len(e.Metadata) > 0 && !json.Valid(e.Metadata) {
		return Invalid("metadata is not valid JSON")
	}
	if !ValidLevel(strings.ToUpper(strings.TrimSpace(e.Level))) {
		return Invalid("unsupported error severity")
	}
	return timestamp(e.CreatedAt, now)
}

func timestamp(value, now time.Time) error {
	if value.IsZero() {
		return Invalid("event timestamp is missing")
	}
	if value.Year() < 1 || value.Year() > 9999 {
		return Invalid("event timestamp is outside the supported range")
	}
	if value.After(now) {
		return Invalid("event timestamp is in the future")
	}
	return nil
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }
