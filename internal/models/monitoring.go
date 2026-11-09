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
