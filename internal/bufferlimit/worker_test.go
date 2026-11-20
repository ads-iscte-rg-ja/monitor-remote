package bufferlimit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"monitor-remote/internal/repository"
)

type fakeEnforcer struct {
	results []repository.BufferEnforcement
	errs    []error
	calls   int
	cancel  context.CancelFunc
}

func (f *fakeEnforcer) EnforceBufferLimit(context.Context, repository.BufferLimits, time.Time) (repository.BufferEnforcement, error) {
	i := f.calls
	f.calls++
	if f.calls == len(f.results) {
		defer f.cancel()
	}
	return f.results[i], f.errs[i]
}

type logs struct {
	mu    sync.Mutex
	lines []string
}

func (l *logs) printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func TestRunLogsOnlyCyclesThatChangedTheBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	enforcer := &fakeEnforcer{
		results: []repository.BufferEnforcement{
			{},
			{},
			{SizeBefore: 10, SizeAfter: 5, Delivered: 2, Dropped: map[string]int64{"INFO": 3}},
			{SizeBefore: 10, SizeAfter: 5, Dropped: map[string]int64{"ERROR": 1}, HardLimit: true},
			{SizeBefore: 10, SizeAfter: 10},
		},
		errs:   []error{nil, errors.New("disk I/O error"), nil, nil, nil},
		cancel: cancel,
	}
	var out logs
	done := make(chan struct{})
	go func() {
		Run(ctx, enforcer, repository.BufferLimits{Soft: 1, Hard: 1}, time.Millisecond, out.printf)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
	if enforcer.calls != 5 {
		t.Fatalf("%d enforcement cycles", enforcer.calls)
	}
	want := []string{"disk I/O error", "dropped INFO=3", "CRITICAL hard limit reached"}
	if len(out.lines) != len(want) {
		t.Fatalf("logs %q", out.lines)
	}
	for i, fragment := range want {
		if !strings.Contains(out.lines[i], fragment) {
			t.Fatalf("log %d = %q, want %q", i, out.lines[i], fragment)
		}
	}
}

func TestRunStopsWithoutLoggingWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	enforcer := &fakeEnforcer{
		results: []repository.BufferEnforcement{{}},
		errs:    []error{context.Canceled},
		cancel:  cancel,
	}
	var out logs
	Run(ctx, enforcer, repository.BufferLimits{Soft: 1, Hard: 1}, time.Hour, out.printf)
	if enforcer.calls != 1 || len(out.lines) != 0 {
		t.Fatalf("calls=%d logs=%q", enforcer.calls, out.lines)
	}
}
