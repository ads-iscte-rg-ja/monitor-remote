package collect

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"monitor-remote/internal/models"
)

type fakeErrorLogWriter struct {
	savedErrorLog *models.ErrorLog
	err           error
}

func (f *fakeErrorLogWriter) SaveErrorLog(
	ctx context.Context,
	errorLog *models.ErrorLog,
) error {
	f.savedErrorLog = errorLog
	return f.err
}

func TestErrorLogCollectorCollect(
	t *testing.T,
) {
	writer := &fakeErrorLogWriter{}
	collector := NewErrorLogCollector(writer)

	createdAt := time.Date(
		2026,
		time.September,
		1,
		10,
		30,
		0,
		0,
		time.FixedZone("WEST", 60*60),
	)

	errorLog, err := collector.Collect(
		context.Background(),
		" payment-api ",
		" production ",
		" error ",
		" Failed to process payment ",
		WithStackTrace(" stack trace "),
		WithMetadata(map[string]any{
			"order_id": "123",
		}),
		WithCreatedAt(createdAt),
	)
	if err != nil {
		t.Fatalf(
			"Collect returned an error: %v",
			err,
		)
	}

	// The database assigns the source ID; the collector does not generate one.
	if errorLog.ID != 0 {
		t.Error("collector must leave ID assignment to SQLite")
	}

	if errorLog.AppName != "payment-api" {
		t.Errorf(
			"AppName = %q; expected payment-api",
			errorLog.AppName,
		)
	}

	if errorLog.Environment != "production" {
		t.Errorf(
			"Environment = %q; expected production",
			errorLog.Environment,
		)
	}

	if errorLog.Level != "ERROR" {
		t.Errorf(
			"Level = %q; expected ERROR",
			errorLog.Level,
		)
	}

	if errorLog.Message != "Failed to process payment" {
		t.Errorf(
			"Message = %q; unexpected value",
			errorLog.Message,
		)
	}

	if errorLog.StackTrace == nil ||
		*errorLog.StackTrace != "stack trace" {
		t.Errorf(
			"StackTrace = %v; expected stack trace",
			errorLog.StackTrace,
		)
	}

	expectedCreatedAt := createdAt.UTC()
	if !errorLog.CreatedAt.Equal(expectedCreatedAt) {
		t.Errorf(
			"CreatedAt = %s; expected %s",
			errorLog.CreatedAt,
			expectedCreatedAt,
		)
	}

	var metadata map[string]any
	if err := json.Unmarshal(
		errorLog.Metadata,
		&metadata,
	); err != nil {
		t.Fatalf(
			"invalid metadata JSON: %v",
			err,
		)
	}

	if metadata["order_id"] != "123" {
		t.Errorf(
			"metadata order_id = %v; expected 123",
			metadata["order_id"],
		)
	}

	if writer.savedErrorLog != errorLog {
		t.Error(
			"writer did not receive the collected error",
		)
	}
}

func TestErrorLogCollectorRejectsInvalidInput(
	t *testing.T,
) {
	tests := []struct {
		name        string
		appName     string
		environment string
		level       string
		message     string
	}{
		{
			name:        "missing application",
			environment: "production",
			level:       "ERROR",
			message:     "failure",
		},
		{
			name:    "missing environment",
			appName: "payment-api",
			level:   "ERROR",
			message: "failure",
		},
		{
			name:        "invalid level",
			appName:     "payment-api",
			environment: "production",
			level:       "DEBUG",
			message:     "failure",
		},
		{
			name:        "missing message",
			appName:     "payment-api",
			environment: "production",
			level:       "ERROR",
		},
	}

	for _, test := range tests {
		t.Run(
			test.name,
			func(t *testing.T) {
				collector := NewErrorLogCollector(
					&fakeErrorLogWriter{},
				)

				_, err := collector.Collect(
					context.Background(),
					test.appName,
					test.environment,
					test.level,
					test.message,
				)
				if err == nil {
					t.Fatal(
						"expected an error",
					)
				}
			},
		)
	}
}

func TestErrorLogCollectorReturnsWriterError(
	t *testing.T,
) {
	expectedError := errors.New("database unavailable")

	collector := NewErrorLogCollector(
		&fakeErrorLogWriter{
			err: expectedError,
		},
	)

	_, err := collector.Collect(
		context.Background(),
		"payment-api",
		"production",
		"CRITICAL",
		"database connection failed",
	)
	if !errors.Is(err, expectedError) {
		t.Fatalf(
			"error = %v; expected %v",
			err,
			expectedError,
		)
	}
}
