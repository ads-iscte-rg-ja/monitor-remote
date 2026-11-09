package requests

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"monitor-remote/internal/validation"
)

// DeliveryError describes a delivery-wide problem, never a reason to quarantine
// an otherwise valid record. Paused delivery is resumed by fixing config/restarting.
type DeliveryError struct {
	StatusCode int
	Pause      bool
	RetryAfter time.Duration
}

func (e *DeliveryError) Error() string {
	if e.Pause {
		return fmt.Sprintf("central returned HTTP %d; delivery paused until restart", e.StatusCode)
	}
	return fmt.Sprintf("central returned HTTP %d", e.StatusCode)
}

func responseFailure(response *http.Response) error {
	status := response.StatusCode
	if status == http.StatusBadRequest || status == http.StatusRequestEntityTooLarge {
		// Only the documented central record-specific rejection codes authorize
		// quarantine. A proxy/plain-text 400/413 is a configuration failure.
		var rejection struct {
			Error    string `json:"error"`
			Message  string `json:"message"`
			SourceID int64  `json:"source_id"`
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, 8193))
		if err != nil {
			return fmt.Errorf("read central rejection: %w", err)
		}
		mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if mediaErr == nil && mediaType == "application/json" && len(body) <= 8192 && json.Unmarshal(body, &rejection) == nil {
			if status == http.StatusBadRequest && rejection.Error == "invalid_data" && rejection.SourceID > 0 {
				return &validation.RecordError{SourceID: rejection.SourceID, Reason: safeRecordReason(rejection.Message)}
			}
			if status == http.StatusRequestEntityTooLarge && rejection.Error == "body_too_large" {
				return ErrBatchBodyTooLarge
			}
		}
	}
	e := &DeliveryError{StatusCode: status}
	if (status >= 200 && status < 300) || status == http.StatusTooManyRequests || status >= 500 || status == http.StatusRequestTimeout {
		e.RetryAfter = retryAfter(response.Header.Get("Retry-After"), time.Now())
		return e
	}
	// Redirects, authentication/access failures, and unexpected contract/status
	// errors must not repeatedly consume the queue or quarantine every payload.
	e.Pause = true
	return e
}

func safeRecordReason(message string) string {
	// Never persist arbitrary upstream response text: it can contain telemetry or
	// echoed credentials. These messages are the central's documented constants.
	switch message {
	case "timestamp cannot be in the future", "monitoring field exceeds its size limit",
		"CPU percent must be between 0 and 100", "invalid RAM usage", "invalid disk usage",
		"error ID is required", "app name is required", "environment is required",
		"invalid error level", "error message is required":
		return "central rejected record: " + message
	default:
		return "central rejected record: invalid_data"
	}
}

func retryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		const maxDuration = time.Duration(1<<63 - 1)
		if seconds > int64(maxDuration/time.Second) {
			return maxDuration
		}
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(value); err == nil && date.After(now) {
		return date.Sub(now)
	}
	return 0
}
