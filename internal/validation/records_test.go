package validation

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"monitor-remote/internal/models"
)

func TestErrorValidationRejectsUnsendableRecordsWithoutChangingThem(t *testing.T) {
	now := time.Now().UTC()
	for _, change := range []func(*models.ErrorLog){
		func(e *models.ErrorLog) { e.Message = strings.Repeat("x", 16385) },
		func(e *models.ErrorLog) { e.AppName = strings.Repeat("a", 201) },
		func(e *models.ErrorLog) { e.Environment = "" },
		func(e *models.ErrorLog) { e.CreatedAt = now.Add(time.Hour) },
		func(e *models.ErrorLog) { e.CreatedAt = time.Time{} },
		func(e *models.ErrorLog) { e.Metadata = json.RawMessage(`{"broken"`) },
	} {
		e := models.ErrorLog{ID: 1, AppName: "app", Environment: "test", Level: "ERROR", Message: "failure", CreatedAt: now, Metadata: json.RawMessage(`{}`)}
		change(&e)
		original := e.Message
		var invalid *RecordError
		if err := ErrorLog(e, now); !errors.As(err, &invalid) {
			t.Fatal("invalid record accepted", err)
		}
		if e.Message != original {
			t.Fatal("record was rewritten")
		}
	}
}

func TestStatisticsValidation(t *testing.T) {
	now := time.Now().UTC()
	s := models.MachineStat{ID: 1, CPUPercent: 50, RAMUsedMB: 1, RAMTotalMB: 2, DiskUsedGB: 1, DiskTotalGB: 2, CollectedAt: now}
	if err := MachineStat(s, now); err != nil {
		t.Fatal(err)
	}
	s.CPUPercent = math.Inf(1)
	if err := MachineStat(s, now); err == nil {
		t.Fatal("infinite CPU accepted")
	}
	s.CPUPercent = 50
	s.RAMUsedMB = 3
	if err := MachineStat(s, now); err == nil {
		t.Fatal("impossible RAM usage accepted")
	}
}
