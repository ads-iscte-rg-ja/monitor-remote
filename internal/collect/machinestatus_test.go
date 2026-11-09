package collect

import (
	"context"
	"testing"
	"time"
)

func TestMachineStatusCollectorCollect(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	collector := NewMachineStatusCollector("/")

	beforeCollection := time.Now().UTC()

	stat, err := collector.Collect(ctx)
	if err != nil {
		t.Fatalf(
			"Collect returned an error: %v",
			err,
		)
	}

	if stat.CPUPercent < 0 ||
		stat.CPUPercent > 100 {
		t.Errorf(
			"CPUPercent = %.2f; expected a value between 0 and 100",
			stat.CPUPercent,
		)
	}

	if stat.RAMTotalMB <= 0 {
		t.Errorf(
			"RAMTotalMB = %.2f; expected a positive value",
			stat.RAMTotalMB,
		)
	}

	if stat.RAMUsedMB < 0 ||
		stat.RAMUsedMB > stat.RAMTotalMB {
		t.Errorf(
			"RAMUsedMB = %.2f; expected a value between 0 and %.2f",
			stat.RAMUsedMB,
			stat.RAMTotalMB,
		)
	}

	if stat.DiskTotalGB <= 0 {
		t.Errorf(
			"DiskTotalGB = %.2f; expected a positive value",
			stat.DiskTotalGB,
		)
	}

	if stat.DiskUsedGB < 0 ||
		stat.DiskUsedGB > stat.DiskTotalGB {
		t.Errorf(
			"DiskUsedGB = %.2f; expected a value between 0 and %.2f",
			stat.DiskUsedGB,
			stat.DiskTotalGB,
		)
	}

	if stat.CollectedAt.Before(beforeCollection) {
		t.Errorf(
			"CollectedAt = %s; expected a current timestamp",
			stat.CollectedAt,
		)
	}

	if stat.CollectedAt.After(time.Now().UTC()) {
		t.Errorf(
			"CollectedAt = %s; timestamp is in the future",
			stat.CollectedAt,
		)
	}
}
