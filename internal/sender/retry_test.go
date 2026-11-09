package sender

import (
	"context"
	"errors"
	"testing"
	"time"

	"monitor-remote/internal/requests"
)

func TestRetryDelaysAndCancellation(t *testing.T) {
	for failures := 1; failures <= 100; failures++ {
		if delay := retryDelay(failures); delay <= 0 || delay > 5*time.Minute {
			t.Fatalf("unbounded retry delay %s", delay)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Sender{}
	calls, reports := 0, 0
	s.run(ctx, time.Second, func(context.Context) error { calls++; return errors.New("temporary connection failure") }, func(error) { reports++; cancel() })
	if calls != 1 || reports != 1 {
		t.Fatal("cancellation did not stop retry work")
	}
}

func TestRetryAfterIsSharedAndNotCappedByBackoff(t *testing.T) {
	s := &Sender{}
	s.observeFailure(&requests.DeliveryError{StatusCode: 429, RetryAfter: time.Hour})
	var limited *requests.DeliveryError
	if err := s.deliveryGate(); !errors.As(err, &limited) || limited.RetryAfter < 59*time.Minute {
		t.Fatal("Retry-After was ignored or capped", err)
	}
	// A shorter second response must never shorten an already imposed cooldown.
	s.observeFailure(&requests.DeliveryError{StatusCode: 429, RetryAfter: time.Second})
	if err := s.deliveryGate(); !errors.As(err, &limited) || limited.RetryAfter < 59*time.Minute {
		t.Fatal("cooldown was shortened", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s.wait(ctx, 0) {
		t.Fatal("canceled wait allowed another delivery")
	}
}
