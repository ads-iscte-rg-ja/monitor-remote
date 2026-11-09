package collect

import (
	"context"
	"fmt"
	"time"

	"monitor-remote/internal/models"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
)

const (
	bytesPerMegabyte = 1024 * 1024
	bytesPerGigabyte = 1024 * 1024 * 1024
)

type MachineStatusCollector struct {
	diskPath string
}

func NewMachineStatusCollector(
	diskPath string,
) *MachineStatusCollector {
	return &MachineStatusCollector{
		diskPath: diskPath,
	}
}

func (c *MachineStatusCollector) Collect(
	ctx context.Context,
) (*models.MachineStat, error) {
	cpuPercentages, err := cpu.PercentWithContext(
		ctx,
		time.Second,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"collect CPU usage: %w",
			err,
		)
	}

	if len(cpuPercentages) == 0 {
		return nil, fmt.Errorf(
			"collect CPU usage: no value returned",
		)
	}

	memory, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"collect RAM usage: %w",
			err,
		)
	}

	diskUsage, err := disk.UsageWithContext(
		ctx,
		c.diskPath,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"collect disk usage for %s: %w",
			c.diskPath,
			err,
		)
	}

	return &models.MachineStat{
		CPUPercent: cpuPercentages[0],

		RAMUsedMB: float64(memory.Used) /
			bytesPerMegabyte,
		RAMTotalMB: float64(memory.Total) /
			bytesPerMegabyte,

		DiskUsedGB: float64(diskUsage.Used) /
			bytesPerGigabyte,
		DiskTotalGB: float64(diskUsage.Total) /
			bytesPerGigabyte,

		CollectedAt: time.Now().UTC(),
	}, nil
}
