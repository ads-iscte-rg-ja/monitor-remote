package validation

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	bufferschema "monitor-remote/database"
)

// Both CHECKs of the buffer schema accept exactly Levels (buffer_drops also
// counts machine statistics). Changing one side without the other fails here.
func TestLevelsMatchTheBufferSchema(t *testing.T) {
	for _, tc := range []struct{ column, extra string }{{"level", ""}, {"kind", "machine_stat"}} {
		m := regexp.MustCompile(tc.column + ` TEXT (?:NOT NULL|PRIMARY KEY) CHECK\(` + tc.column + ` IN \(([^)]*)\)\)`).FindStringSubmatch(bufferschema.SQL)
		if m == nil {
			t.Fatalf("CHECK on %s not found", tc.column)
		}
		var checked []string
		for _, value := range strings.Split(m[1], ",") {
			if value = strings.Trim(strings.TrimSpace(value), "'"); value != tc.extra {
				checked = append(checked, value)
			}
		}
		if got, want := slices.Sorted(slices.Values(Levels)), slices.Sorted(slices.Values(checked)); !slices.Equal(got, want) {
			t.Fatalf("%s: Go %v, schema %v", tc.column, got, want)
		}
	}
	if !ValidLevel(LevelPanic) || ValidLevel("panic") || ValidLevel("DEBUG") {
		t.Fatal("ValidLevel")
	}
}
