package requests

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"monitor-remote/internal/models"
	"monitor-remote/internal/validation"
)

type errorLogPayload struct {
	ID          int64           `json:"id"`
	AppName     string          `json:"app_name"`
	Environment string          `json:"environment"`
	Level       string          `json:"level"`
	Message     string          `json:"message"`
	StackTrace  *string         `json:"stack_trace,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

type errorLogsPayload struct {
	Errors []errorLogPayload `json:"errors"`
}

func (c *Client) SendErrorLogs(
	ctx context.Context,
	logs []models.ErrorLog,
) error {
	if len(logs) == 0 || len(logs) > MaxBatchRecords {
		return fmt.Errorf("error batch must contain 1 to %d records", MaxBatchRecords)
	}
	payload := errorLogsPayload{Errors: make([]errorLogPayload, 0, len(logs))}
	want := make(map[int64]bool, len(logs))
	for _, entry := range logs {
		if err := validation.ErrorLog(entry, time.Now().UTC()); err != nil {
			return &validation.RecordError{SourceID: entry.ID, Reason: err.Error()}
		}
		if want[entry.ID] {
			return fmt.Errorf("duplicate error source ID in batch")
		}
		want[entry.ID] = true
		payload.Errors = append(payload.Errors, errorLogPayload{ID: entry.ID, AppName: entry.AppName, Environment: entry.Environment, Level: entry.Level, Message: entry.Message, StackTrace: entry.StackTrace, Metadata: entry.Metadata, CreatedAt: entry.CreatedAt.UTC()})
	}

	var ack struct {
		Accepted   *int    `json:"accepted"`
		Inserted   *int    `json:"inserted"`
		Duplicates *int    `json:"duplicates"`
		SourceIDs  []int64 `json:"source_ids"`
	}
	if err := c.postJSON(
		ctx,
		"/api/v1/ingest/errors",
		payload,
		&ack,
	); err != nil {
		return err
	}
	if ack.Accepted == nil || ack.Inserted == nil || ack.Duplicates == nil || *ack.Accepted != len(logs) || *ack.Inserted < 0 || *ack.Inserted > len(logs) || *ack.Duplicates < 0 || *ack.Duplicates > len(logs) || *ack.Inserted+*ack.Duplicates != len(logs) || len(ack.SourceIDs) != len(logs) {
		return fmt.Errorf("central did not acknowledge the complete error submission")
	}
	for _, id := range ack.SourceIDs {
		if !want[id] {
			return fmt.Errorf("central acknowledgement does not match the submitted error source IDs")
		}
		delete(want, id)
	}
	return nil
}
