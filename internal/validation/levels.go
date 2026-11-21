package validation

// Log levels, the only list in the remote (ADR-0015). The buffer schema's CHECKs
// and the protocol mapping are compared with it by tests.
const (
	LevelInfo     = "INFO"
	LevelWarn     = "WARN"
	LevelError    = "ERROR"
	LevelCritical = "CRITICAL"
	LevelPanic    = "PANIC"
)

// Levels lists every level, from the least to the most severe.
var Levels = []string{LevelInfo, LevelWarn, LevelError, LevelCritical, LevelPanic}

// ValidLevel reports whether level is one of Levels (exact, upper case).
func ValidLevel(level string) bool {
	for _, known := range Levels {
		if level == known {
			return true
		}
	}
	return false
}
