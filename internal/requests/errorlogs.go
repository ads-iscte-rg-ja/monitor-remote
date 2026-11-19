package requests

import (
	"context"
	"fmt"
	"strings"
	"time"

	ingestv1 "github.com/ads-iscte-rg-ja/monitor-api/gen/go/monitor/ingest/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"monitor-remote/internal/models"
	"monitor-remote/internal/validation"
)

var logLevels = map[string]ingestv1.LogLevel{
	"INFO":     ingestv1.LogLevel_LOG_LEVEL_INFO,
	"WARN":     ingestv1.LogLevel_LOG_LEVEL_WARN,
	"ERROR":    ingestv1.LogLevel_LOG_LEVEL_ERROR,
	"CRITICAL": ingestv1.LogLevel_LOG_LEVEL_CRITICAL,
	"PANIC":    ingestv1.LogLevel_LOG_LEVEL_PANIC,
}

// SendErrorLogs delivers one batch and succeeds only if central accounted for
// every submitted source ID exactly once.
func (c *Client) SendErrorLogs(ctx context.Context, logs []models.ErrorLog) error {
	if len(logs) == 0 || len(logs) > MaxBatchRecords {
		return fmt.Errorf("error batch must contain 1 to %d records", MaxBatchRecords)
	}
	req := &ingestv1.IngestErrorsRequest{Errors: make([]*ingestv1.ErrorLog, 0, len(logs))}
	want := make(map[int64]bool, len(logs))
	for _, entry := range logs {
		if err := validation.ErrorLog(entry, time.Now().UTC()); err != nil {
			return &validation.RecordError{SourceID: entry.ID, Reason: err.Error()}
		}
		if want[entry.ID] {
			return fmt.Errorf("duplicate error source ID in batch")
		}
		want[entry.ID] = true
		req.Errors = append(req.Errors, &ingestv1.ErrorLog{
			SourceId:     entry.ID,
			AppName:      entry.AppName,
			Environment:  entry.Environment,
			Level:        logLevels[strings.ToUpper(strings.TrimSpace(entry.Level))], // validated above
			Message:      entry.Message,
			StackTrace:   entry.StackTrace,
			MetadataJson: string(entry.Metadata),
			CreatedAt:    timestamppb.New(entry.CreatedAt.UTC()),
		})
	}
	resp, err := call(ctx, c, req, c.api.IngestErrors)
	if err != nil {
		return err
	}
	accepted, inserted, duplicates := int(resp.GetAccepted()), int(resp.GetInserted()), int(resp.GetDuplicates())
	if accepted != len(logs) || inserted < 0 || duplicates < 0 || inserted+duplicates != len(logs) || len(resp.GetSourceIds()) != len(logs) {
		return fmt.Errorf("central did not acknowledge the complete error submission")
	}
	for _, id := range resp.GetSourceIds() {
		if !want[id] {
			return fmt.Errorf("central acknowledgement does not match the submitted error source IDs")
		}
		delete(want, id)
	}
	return nil
}
