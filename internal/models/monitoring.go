package models

import (
	"encoding/json"
	"time"
)

type MachineStat struct {
	ID          int64
	CPUPercent  float64
	RAMUsedMB   float64
	RAMTotalMB  float64
	DiskUsedGB  float64
	DiskTotalGB float64
	CollectedAt time.Time
}

type ErrorLog struct {
	ID          int64
	AppName     string
	Environment string
	Level       string
	Message     string
	StackTrace  *string
	Metadata    json.RawMessage
	CreatedAt   time.Time
}

type DeliveryStatus struct {
	ID               int64
	RecordType       string
	RecordID         int64
	QueuedAt         time.Time
	SentAt           *time.Time
	QuarantineReason *string
	AttemptCount     int
	LastAttemptAt    *time.Time
}

const (
	RecordTypeMachineStat = "machine_stat"
	RecordTypeErrorLog    = "error_log"
)

// BufferStatus is a snapshot of the local buffer, reported to central with the
// statistics (ADR-0010). DroppedByKind holds the cumulative drops of ADR-0009.
type BufferStatus struct {
	ObservedAt              time.Time
	Pending                 int64
	Quarantined             int64
	OldestPendingAgeSeconds int64
	DeliveryPaused          bool
	SizeBytes               int64
	DroppedByKind           map[string]int64
}
