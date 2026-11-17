package reconciler

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"crobat/internal/app"
)

type store struct {
	app.JobStore
	mu      sync.Mutex
	results []int64 // returned by successive calls; then 0
	calls   int
	err     error
	limits  []int
}

func (s *store) ReleaseExpired(_ context.Context, now time.Time, limit int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.limits = append(s.limits, limit)
	if time.Since(now) > time.Second {
		return 0, errors.New("stale now")
	}
	if s.err != nil {
		return 0, s.err
	}
	if len(s.results) == 0 {
		return 0, nil
	}
	n := s.results[0]
	s.results = s.results[1:]
	return n, nil
}

func runUntil(t *testing.T, s *store, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go (&Service{Jobs: s, Interval: time.Hour, Batch: 1000, Log: slog.New(slog.DiscardHandler)}).Run(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		done := cond()
		s.mu.Unlock()
		if done {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}

func TestSweepsAgainImmediatelyWhileBatchesAreFull(t *testing.T) {
	s := &store{results: []int64{1000, 1000, 7}}
	runUntil(t, s, func() bool { return s.calls == 3 }) // interval is 1h: only a full batch can trigger this
	time.Sleep(20 * time.Millisecond)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.calls != 3 {
		t.Fatalf("a partial batch must wait for the next tick, got %d calls", s.calls)
	}
	if s.limits[0] != 1000 {
		t.Fatalf("limit = %d", s.limits[0])
	}
}

func TestKeepsRunningAfterAnError(t *testing.T) {
	s := &store{err: errors.New("db down")}
	runUntil(t, s, func() bool { return s.calls >= 1 })
}
