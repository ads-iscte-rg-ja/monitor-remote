package requests

import (
	"context"
	"fmt"
	"time"

	ingestv1 "github.com/ads-iscte-rg-ja/monitor-api/gen/go/monitor/ingest/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"monitor-remote/internal/models"
	"monitor-remote/internal/validation"
)

// SendMachineStats delivers one batch and succeeds only if central acknowledged
// every submitted source ID, once, for a single host.
func (c *Client) SendMachineStats(ctx context.Context, stats []models.MachineStat) error {
	if len(stats) == 0 || len(stats) > MaxBatchRecords {
		return fmt.Errorf("statistics batch must contain 1 to %d records", MaxBatchRecords)
	}
	req := &ingestv1.IngestStatsRequest{Stats: make([]*ingestv1.MachineStat, 0, len(stats))}
	want := make(map[int64]bool, len(stats))
	for _, stat := range stats {
		if err := validation.MachineStat(stat, time.Now().UTC()); err != nil {
			return &validation.RecordError{SourceID: stat.ID, Reason: err.Error()}
		}
		if want[stat.ID] {
			return fmt.Errorf("duplicate statistics source ID in batch")
		}
		want[stat.ID] = true
		req.Stats = append(req.Stats, &ingestv1.MachineStat{
			SourceId:    stat.ID,
			CpuPercent:  stat.CPUPercent,
			RamUsedMb:   stat.RAMUsedMB,
			RamTotalMb:  stat.RAMTotalMB,
			DiskUsedGb:  stat.DiskUsedGB,
			DiskTotalGb: stat.DiskTotalGB,
			CollectedAt: timestamppb.New(stat.CollectedAt.UTC()),
		})
	}
	resp, err := call(ctx, c, req, c.api.IngestStats)
	if err != nil {
		return err
	}
	acks := resp.GetAcknowledgements()
	if len(acks) != len(stats) {
		return fmt.Errorf("central did not acknowledge the complete statistics batch")
	}
	var hostID int64
	centralIDs := make(map[int64]bool, len(stats))
	for _, ack := range acks {
		if ack.GetId() <= 0 || ack.GetHostId() <= 0 || !want[ack.GetSourceId()] || centralIDs[ack.GetId()] || (hostID != 0 && hostID != ack.GetHostId()) {
			return fmt.Errorf("central acknowledgement does not match the submitted statistics source IDs")
		}
		hostID = ack.GetHostId()
		centralIDs[ack.GetId()] = true
		delete(want, ack.GetSourceId())
	}
	return nil
}
