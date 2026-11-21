package requests

import (
	"testing"

	ingestv1 "github.com/ads-iscte-rg-ja/monitor-api/gen/go/monitor/ingest/v1"

	"monitor-remote/internal/validation"
)

// Every level has a translation to the contract, and every known contract level
// comes from one (ADR-0015).
func TestEveryLevelHasAContractTranslation(t *testing.T) {
	if len(logLevels) != len(validation.Levels) {
		t.Fatalf("%d translations for %d levels", len(logLevels), len(validation.Levels))
	}
	seen := map[ingestv1.LogLevel]bool{}
	for _, level := range validation.Levels {
		translated, ok := logLevels[level]
		if !ok || translated == ingestv1.LogLevel_LOG_LEVEL_UNSPECIFIED || seen[translated] {
			t.Fatalf("level %s: %v", level, translated)
		}
		seen[translated] = true
	}
	for value := range ingestv1.LogLevel_name {
		if level := ingestv1.LogLevel(value); level != ingestv1.LogLevel_LOG_LEVEL_UNSPECIFIED && !seen[level] {
			t.Fatalf("contract level %s has no remote level", level)
		}
	}
}
