package sender

import (
	"context"
	"errors"
	"os"
	"time"

	"google.golang.org/grpc/codes"

	"monitor-remote/internal/requests"
)

// DefaultProbeInterval is the wait between probes while delivery is paused (ADR-0011).
const DefaultProbeInterval = 5 * time.Minute

var errPaused = errors.New("central delivery paused; waiting for the next probe")

// admission is what one delivery step may send: a full batch, or one record
// when it is the probe of a paused delivery.
type admission struct {
	limit int
	probe bool
}

// admit decides the next delivery step. While paused, only one probe runs at a
// time for both categories (they share the host key): the caller that claims it
// already books the next one, so the other category sleeps until then.
func (s *Sender) admit() (admission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	if s.paused {
		if s.probing || now.Before(s.probeAt) {
			return admission{}, errPaused
		}
		s.probing = true
		s.probeAt = now.Add(s.interval())
		return admission{limit: 1, probe: true}, nil
	}
	if delay := s.notBefore.Sub(now); delay > 0 {
		return admission{}, &requests.DeliveryError{Code: codes.ResourceExhausted, RetryAfter: delay}
	}
	return admission{limit: s.batchLimit}, nil
}

// deliveryGate is checked before each batch of a normal step: another category
// may have paused delivery or received a Retry-After meanwhile.
func (s *Sender) deliveryGate() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paused {
		return errPaused
	}
	if delay := s.notBefore.Sub(s.clock()); delay > 0 {
		return &requests.DeliveryError{Code: codes.ResourceExhausted, RetryAfter: delay}
	}
	return nil
}

func (s *Sender) observeFailure(err error) {
	var deliveryErr *requests.DeliveryError
	if !errors.As(err, &deliveryErr) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	if deliveryErr.Pause && !s.paused {
		s.paused = true
		s.probeAt = now.Add(s.interval())
		s.log("delivery paused: central returned %s; next probe in %s", deliveryErr.Code, s.interval())
	}
	if deliveryErr.RetryAfter > 0 {
		next := now.Add(deliveryErr.RetryAfter)
		if next.After(s.notBefore) {
			s.notBefore = next
		}
	}
}

// resume ends a pause: central accepted (or validated and rejected) the probe,
// so the key and the contract are good. Both categories wake up.
func (s *Sender) resume() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.paused {
		return
	}
	s.paused, s.probing = false, false
	s.log("delivery resumed after a successful probe")
	s.broadcast()
}

// probeSucceeded resumes delivery if the step that central answered was a probe.
func (s *Sender) probeSucceeded(probe bool) {
	if probe {
		s.resume()
	}
}

// finishProbe ends a probe that did not resume delivery; the next one is
// already booked.
func (s *Sender) finishProbe() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probing = false
}

// releaseProbe gives the probe back unused (nothing to send in this category),
// so the other category can probe at once.
func (s *Sender) releaseProbe() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probing = false
	s.probeAt = s.clock()
	s.broadcast()
}

// ProbeNow brings the next probe forward, after the credential was reloaded.
func (s *Sender) ProbeNow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paused && !s.probing {
		s.probeAt = s.clock()
		s.broadcast()
	}
}

// ReloadCredential applies a host key read again (SIGHUP) and probes at once.
func (s *Sender) ReloadCredential(load func() (string, error)) error {
	key, err := load()
	if err != nil {
		return err
	}
	if err := s.client.SetAPIKey(key); err != nil {
		return err
	}
	s.ProbeNow()
	return nil
}

// ReloadOnSignal reloads the credential at each signal until ctx ends.
func (s *Sender) ReloadOnSignal(ctx context.Context, signals <-chan os.Signal, load func() (string, error)) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-signals:
			err := s.ReloadCredential(load)
			s.mu.Lock()
			if err != nil {
				s.log("SIGHUP: host key not reloaded: %v", err)
			} else {
				s.log("SIGHUP: host key reloaded")
			}
			s.mu.Unlock()
		}
	}
}

// SetLogger receives pause and resume messages.
func (s *Sender) SetLogger(logf func(string, ...any)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logf = logf
}

// The helpers below are called with s.mu held.

func (s *Sender) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *Sender) interval() time.Duration {
	if s.probeInterval > 0 {
		return s.probeInterval
	}
	return DefaultProbeInterval
}

func (s *Sender) log(format string, args ...any) {
	if s.logf != nil {
		s.logf(format, args...)
	}
}

// wakeup returns the channel closed at the next state change.
func (s *Sender) wakeup() <-chan struct{} {
	if s.wake == nil {
		s.wake = make(chan struct{})
	}
	return s.wake
}

func (s *Sender) broadcast() {
	if s.wake != nil {
		close(s.wake)
	}
	s.wake = make(chan struct{})
}

// wait sleeps for delay, then until delivery may run: after a Retry-After, or
// until the next probe while paused. A resume or ProbeNow wakes it early.
func (s *Sender) wait(ctx context.Context, delay time.Duration) bool {
	if delay > 0 && !waitDuration(ctx, delay, nil) {
		return false
	}
	for ctx.Err() == nil {
		s.mu.Lock()
		until := s.notBefore
		if s.paused {
			until = s.probeAt
		}
		remaining := until.Sub(s.clock())
		wake := s.wakeup()
		s.mu.Unlock()
		if remaining <= 0 {
			return true
		}
		if !waitDuration(ctx, remaining, wake) {
			return false
		}
	}
	return false
}

func waitDuration(ctx context.Context, duration time.Duration, wake <-chan struct{}) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
	case <-wake:
	}
	return ctx.Err() == nil
}
