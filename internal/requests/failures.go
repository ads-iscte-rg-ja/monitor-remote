package requests

import (
	"fmt"
	"strings"
	"time"

	ingestv1 "github.com/ads-iscte-rg-ja/monitor-api/gen/go/monitor/ingest/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"monitor-remote/internal/validation"
)

// DeliveryError describes a delivery-wide problem, never a reason to quarantine
// an otherwise valid record. A paused delivery resumes after a successful probe
// (every 5 minutes, or at once after SIGHUP reloads the key; ADR-0011).
type DeliveryError struct {
	Code       codes.Code
	Pause      bool
	RetryAfter time.Duration
}

func (e *DeliveryError) Error() string {
	if e.Pause {
		return fmt.Sprintf("central returned %s; delivery paused until a probe succeeds", e.Code)
	}
	return fmt.Sprintf("central returned %s", e.Code)
}

// classify maps a gRPC error to the sender's four behaviors, using only the
// codes defined by the contract (ADR-0003). The status message is free text and
// is never used to decide anything, nor persisted.
func classify(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return &DeliveryError{Code: codes.Unknown}
	}
	switch st.Code() {
	case codes.InvalidArgument:
		for _, detail := range st.Details() {
			if rejection, ok := detail.(*ingestv1.RecordRejection); ok && rejection.GetSourceId() > 0 {
				return &validation.RecordError{SourceID: rejection.GetSourceId(), Reason: recordReason(rejection.GetReason())}
			}
		}
		// A malformed batch is a contract violation: pause rather than quarantine.
		return &DeliveryError{Code: st.Code(), Pause: true}
	case codes.ResourceExhausted:
		if delay, ok := retryInfo(st); ok {
			return &DeliveryError{Code: st.Code(), RetryAfter: delay}
		}
		// Without RetryInfo it is the message size limit: split the batch.
		return ErrBatchBodyTooLarge
	case codes.Unavailable, codes.Internal, codes.DeadlineExceeded, codes.Aborted, codes.Unknown, codes.Canceled:
		delay, _ := retryInfo(st)
		return &DeliveryError{Code: st.Code(), RetryAfter: delay}
	default:
		// Authentication/permission failures and unexpected codes (e.g. talking
		// to the wrong service) must not repeatedly consume the queue.
		return &DeliveryError{Code: st.Code(), Pause: true}
	}
}

func retryInfo(st *status.Status) (time.Duration, bool) {
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.RetryInfo); ok {
			delay := info.GetRetryDelay().AsDuration()
			if delay < 0 {
				delay = 0
			}
			return delay, true
		}
	}
	return 0, false
}

// recordReason builds the persisted quarantine reason from the stable code
// only, so no upstream text (which could echo telemetry) is ever stored.
func recordReason(reason ingestv1.RejectionReason) string {
	name := strings.TrimPrefix(reason.String(), "REJECTION_REASON_")
	return "central rejected record: " + strings.ToLower(name)
}
