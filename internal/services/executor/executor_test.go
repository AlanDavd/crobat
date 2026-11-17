package executor

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"crobat/internal/app"
	"crobat/internal/domain"
)

// queue is an in-memory app.JobStore holding pending jobs.
type queue struct {
	app.JobStore
	mu      sync.Mutex
	pending []domain.Job
	saved   []domain.Job
	workers []string
	fenced  bool
	done    chan struct{}
	want    int
}

func (q *queue) Claim(_ context.Context, worker string, n int, _, _ time.Time) ([]domain.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n = min(n, len(q.pending))
	out := q.pending[:n:n]
	q.pending = q.pending[n:]
	for i := range out {
		out[i].Status, out[i].WorkerID = domain.JobRunning, worker
		out[i].Attempt++
	}
	return out, nil
}

func (q *queue) SaveResult(_ context.Context, j *domain.Job, worker string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.saved = append(q.saved, *j)
	q.workers = append(q.workers, worker)
	if len(q.saved) == q.want {
		close(q.done)
	}
	return !q.fenced, nil
}

type caller func(domain.Job) domain.Result

func (c caller) Call(_ context.Context, j domain.Job) domain.Result { return c(j) }

var ok200 caller = func(domain.Job) domain.Result {
	now := time.Now()
	return domain.Result{HTTPStatus: 200, StartedAt: now, FinishedAt: now}
}

func jobs(n int) []domain.Job {
	var out []domain.Job
	for range n {
		out = append(out, domain.Job{ID: domain.NewID(), Status: domain.JobPending, NextAttemptAt: time.Now(),
			Retry: domain.RetryPolicy{MaxAttempts: 3, BackoffMS: 1000}})
	}
	return out
}

// run starts the executor and waits for q.want results (or fails after 5s).
func run(t *testing.T, q *queue, c app.Caller, workers int, poll time.Duration) {
	t.Helper()
	q.done = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		(&Service{Jobs: q, Caller: c, WorkerID: "w1", Workers: workers, Poll: poll, Log: slog.New(slog.DiscardHandler)}).Run(ctx)
		close(stopped)
	}()
	select {
	case <-q.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("only %d of %d jobs finished", len(q.saved), q.want)
	}
	cancel()
	<-stopped
}

// Regression test for the throughput cap found in load testing: freed worker
// slots must be refilled without waiting for the next poll tick.
func TestRefillsWorkersWithoutWaitingForThePoll(t *testing.T) {
	q := &queue{pending: jobs(50), want: 50}
	start := time.Now()
	run(t, q, ok200, 4, time.Hour) // a tick would never come
	if time.Since(start) > 2*time.Second {
		t.Fatalf("took %v", time.Since(start))
	}
}

func TestFiresOnTheJobsTimer(t *testing.T) {
	due := time.Now().Add(300 * time.Millisecond)
	q := &queue{pending: []domain.Job{{ID: "j1", NextAttemptAt: due, Retry: domain.RetryPolicy{MaxAttempts: 1}}}, want: 1}
	var fired time.Time
	run(t, q, caller(func(domain.Job) domain.Result {
		fired = time.Now()
		return domain.Result{HTTPStatus: 204, StartedAt: fired, FinishedAt: fired}
	}), 1, 10*time.Millisecond)
	if fired.Before(due) {
		t.Fatalf("fired %v early", due.Sub(fired))
	}
}

func TestSavesTheAppliedResultUnderItsOwnWorkerID(t *testing.T) {
	q := &queue{pending: jobs(1), want: 1}
	run(t, q, caller(func(domain.Job) domain.Result {
		now := time.Now()
		return domain.Result{HTTPStatus: 503, StartedAt: now, FinishedAt: now}
	}), 1, 10*time.Millisecond)
	j := q.saved[0]
	if j.Status != domain.JobPending || j.Attempt != 1 || j.HTTPStatus != 503 || !j.NextAttemptAt.After(time.Now()) {
		t.Fatalf("a failed attempt must go back to pending with backoff: %+v", j)
	}
	if q.workers[0] != "w1" {
		t.Fatalf("result saved as %q, the fencing guard needs the claiming worker", q.workers[0])
	}
}

func TestFencedResultDoesNotStopTheExecutor(t *testing.T) {
	q := &queue{pending: jobs(3), want: 3, fenced: true}
	run(t, q, ok200, 1, 10*time.Millisecond)
}
