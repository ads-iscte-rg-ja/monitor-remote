package collect

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"monitor-remote/internal/models"
)

type ErrorLogWriter interface {
	SaveErrorLog(
		ctx context.Context,
		errorLog *models.ErrorLog,
	) error
}

type ErrorLogCollector struct {
	writer ErrorLogWriter
}

type ErrorLogOption func(
	errorLog *models.ErrorLog,
) error

func NewErrorLogCollector(
	writer ErrorLogWriter,
) *ErrorLogCollector {
	return &ErrorLogCollector{
		writer: writer,
	}
}

func (c *ErrorLogCollector) Collect(
	ctx context.Context,
	appName string,
	environment string,
	level string,
	message string,
	options ...ErrorLogOption,
) (*models.ErrorLog, error) {
	if c.writer == nil {
		return nil, fmt.Errorf(
			"error log writer is required",
		)
	}

	appName = strings.TrimSpace(appName)
	if appName == "" {
		return nil, fmt.Errorf(
			"application name is required",
		)
	}

	environment = strings.TrimSpace(environment)
	if environment == "" {
		return nil, fmt.Errorf(
			"environment is required",
		)
	}

	level = strings.ToUpper(
		strings.TrimSpace(level),
	)
	if !validErrorLevel(level) {
		return nil, fmt.Errorf(
			"invalid error level %q",
			level,
		)
	}

	message = strings.TrimSpace(message)
	if message == "" {
		return nil, fmt.Errorf(
			"error message is required",
		)
	}

	errorLog := &models.ErrorLog{
		AppName:     appName,
		Environment: environment,
		Level:       level,
		Message:     message,
		Metadata:    json.RawMessage("{}"),
		CreatedAt:   time.Now().UTC(),
	}

	for _, option := range options {
		if option == nil {
			continue
		}

		if err := option(errorLog); err != nil {
			return nil, err
		}
	}

	if err := c.writer.SaveErrorLog(
		ctx,
		errorLog,
	); err != nil {
		return nil, fmt.Errorf(
			"save collected error log: %w",
			err,
		)
	}

	return errorLog, nil
}

func WithStackTrace(
	stackTrace string,
) ErrorLogOption {
	return func(
		errorLog *models.ErrorLog,
	) error {
		stackTrace = strings.TrimSpace(stackTrace)
		if stackTrace == "" {
			return nil
		}

		errorLog.StackTrace = &stackTrace
		return nil
	}
}

func WithMetadata(
	metadata map[string]any,
) ErrorLogOption {
	return func(
		errorLog *models.ErrorLog,
	) error {
		if metadata == nil {
			return nil
		}

		encodedMetadata, err := json.Marshal(
			metadata,
		)
		if err != nil {
			return fmt.Errorf(
				"encode error metadata: %w",
				err,
			)
		}

		errorLog.Metadata = encodedMetadata
		return nil
	}
}

func WithCreatedAt(
	createdAt time.Time,
) ErrorLogOption {
	return func(
		errorLog *models.ErrorLog,
	) error {
		if createdAt.IsZero() {
			return fmt.Errorf(
				"error creation time cannot be zero",
			)
		}

		errorLog.CreatedAt = createdAt.UTC()
		return nil
	}
}

func validErrorLevel(
	level string,
) bool {
	switch level {
	case "INFO",
		"WARN",
		"ERROR",
		"CRITICAL",
		"PANIC":
		return true
	default:
		return false
	}
}
