package app

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"crobat/internal/domain"
)

// ---------------------------------------------------------------- task service

// TaskService is the write API for tasks.
type TaskService struct{ Tasks TaskStore }

func (s *TaskService) Create(ctx context.Context, tenantID, idemKey string, t domain.Task) (*domain.Task, error) {
	if err := t.Normalize(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	t.ID, t.TenantID, t.IdempotencyKey = domain.NewID(), tenantID, idemKey
	t.Status, t.Version, t.CreatedAt, t.UpdatedAt = domain.TaskActive, 1, now, now
	var first *domain.Job
	if t.Cron == "" && t.At == nil {
		// "now": write the job with the task so it does not wait a scheduler tick.
		j := t.NewJob(now)
		first = &j
	} else {
		t.NextRunAt = t.FirstRun(now)
	}
	return s.Tasks.CreateTask(ctx, &t, first)
}

func (s *TaskService) Get(ctx context.Context, tenantID, id string) (*domain.Task, error) {
	return s.Tasks.GetTask(ctx, tenantID, id)
}

func (s *TaskService) List(ctx context.Context, tenantID string) ([]domain.Task, error) {
	return s.Tasks.ListTasks(ctx, tenantID, 500)
}

// Update replaces the editable fields. Existing jobs are untouched.
func (s *TaskService) Update(ctx context.Context, tenantID, id string, in domain.Task) (*domain.Task, error) {
	t, err := s.Tasks.GetTask(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	if err := in.Normalize(); err != nil {
		return nil, err
	}
	scheduleChanged := in.Cron != t.Cron || in.Timezone != t.Timezone || !timeEq(in.At, t.At)
	t.Name, t.Cron, t.Timezone, t.At = in.Name, in.Cron, in.Timezone, in.At
	t.Priority, t.Request, t.TimeoutMS, t.Retry = in.Priority, in.Request, in.TimeoutMS, in.Retry
	if scheduleChanged {
		t.NextRunAt = t.FirstRun(time.Now().UTC())
	}
	return t, s.save(ctx, t)
}

func (s *TaskService) Pause(ctx context.Context, tenantID, id string) (*domain.Task, error) {
	return s.setStatus(ctx, tenantID, id, domain.TaskPaused)
}

func (s *TaskService) Resume(ctx context.Context, tenantID, id string) (*domain.Task, error) {
	return s.setStatus(ctx, tenantID, id, domain.TaskActive)
}

func (s *TaskService) Delete(ctx context.Context, tenantID, id string) error {
	_, err := s.setStatus(ctx, tenantID, id, domain.TaskDeleted)
	return err
}

func (s *TaskService) setStatus(ctx context.Context, tenantID, id string, st domain.TaskStatus) (*domain.Task, error) {
	t, err := s.Tasks.GetTask(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	if t.Status == domain.TaskDeleted {
		return nil, domain.ErrNotFound
	}
	if st == domain.TaskActive && t.Status == domain.TaskPaused && t.Cron != "" {
		// Runs missed while paused are skipped on purpose; resume from now.
		t.NextRunAt = t.NextAfter(time.Now().UTC())
	}
	if st == domain.TaskDeleted {
		t.NextRunAt = nil
	}
	t.Status = st
	return t, s.save(ctx, t)
}

func (s *TaskService) save(ctx context.Context, t *domain.Task) error {
	t.UpdatedAt = time.Now().UTC()
	return s.Tasks.UpdateTask(ctx, t)
}

func timeEq(a, b *time.Time) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && a.Equal(*b))
}

// ------------------------------------------------------------------- scheduler

// Scheduler turns due tasks into pending jobs ahead of time.
type Scheduler struct {
	Tasks     TaskStore
	Interval  time.Duration // 5s
	Lookahead time.Duration // 30s
	Batch     int
	Log       *slog.Logger
}

// maxCatchUp bounds the jobs one task can create per tick after an outage;
// the rest are created on the following ticks.
const maxCatchUp = 1000

func (s *Scheduler) Tick(ctx context.Context) (int, error) {
	horizon := time.Now().UTC().Add(s.Lookahead)
	return s.Tasks.ScheduleDue(ctx, horizon, s.Batch, func(t *domain.Task) []domain.Job {
		var jobs []domain.Job
		for i := 0; t.NextRunAt != nil && !t.NextRunAt.After(horizon) && i < maxCatchUp; i++ {
			jobs = append(jobs, t.NewJob(*t.NextRunAt))
			t.NextRunAt = t.NextAfter(*t.NextRunAt)
		}
		return jobs
	})
}

func (s *Scheduler) Run(ctx context.Context) {
	every(ctx, s.Interval, func() bool {
		n, err := s.Tick(ctx)
		if err != nil {
			s.Log.Error("schedule tick", "err", err)
			return false
		}
		if n > 0 {
			s.Log.Info("scheduled", "tasks", n)
		}
		return n == s.Batch
	})
}

// -------------------------------------------------------------------- executor

// Executor claims jobs, fires each on its own timer and saves the result.
type Executor struct {
	Jobs     JobStore
	Caller   Caller
	WorkerID string
	Workers  int
	Poll     time.Duration
	Log      *slog.Logger
}

func (e *Executor) Run(ctx context.Context) {
	slots := make(chan struct{}, e.Workers)
	// wake refills slots as soon as a quarter of them free up, instead of
	// waiting for the next poll tick (which capped throughput at Workers/Poll).
	wake := make(chan struct{}, 1)
	var wg sync.WaitGroup
	defer wg.Wait() // drain in-flight jobs on shutdown
	tick := time.NewTicker(e.Poll)
	defer tick.Stop()
	for {
		if free := e.Workers - len(slots); free > 0 {
			now := time.Now().UTC()
			jobs, err := e.Jobs.Claim(ctx, e.WorkerID, free, now.Add(e.Poll), now)
			if err != nil {
				e.Log.Error("claim", "err", err)
			}
			for _, j := range jobs {
				slots <- struct{}{}
				wg.Go(func() {
					e.fire(context.WithoutCancel(ctx), j)
					<-slots
					if len(slots) <= e.Workers*3/4 {
						select {
						case wake <- struct{}{}:
						default:
						}
					}
				})
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-wake:
		}
	}
}

func (e *Executor) fire(ctx context.Context, j domain.Job) {
	time.Sleep(time.Until(j.NextAttemptAt)) // the job's own timer
	r := e.Caller.Call(ctx, j)
	j.Apply(r)
	ok, err := e.Jobs.SaveResult(ctx, &j, e.WorkerID)
	log := e.Log.With("job", j.ID, "task", j.TaskID, "attempt", j.Attempt, "status", j.Status,
		"http_status", r.HTTPStatus, "lag_ms", r.StartedAt.Sub(j.ScheduledAt).Milliseconds())
	switch {
	case err != nil:
		log.Error("save result", "err", err) // lease expiry hands it to the reconciler
	case !ok:
		log.Warn("result fenced off: job no longer ours")
	default:
		log.Info("job done")
	}
}

// ------------------------------------------------------------------ reconciler

// Reconciler returns running jobs with an expired lease to pending (or dead).
type Reconciler struct {
	Jobs     JobStore
	Interval time.Duration
	Batch    int
	Log      *slog.Logger
}

func (r *Reconciler) Run(ctx context.Context) {
	every(ctx, r.Interval, func() bool {
		n, err := r.Jobs.ReleaseExpired(ctx, time.Now().UTC(), r.Batch)
		if err != nil {
			r.Log.Error("release expired", "err", err)
			return false
		}
		if n > 0 {
			r.Log.Warn("released expired leases", "jobs", n)
		}
		return n == int64(r.Batch)
	})
}

// -------------------------------------------------------------------- archiver

// Archiver copies finished jobs to history, then deletes them from MySQL.
type Archiver struct {
	Jobs          JobStore
	History       HistoryStore
	Interval      time.Duration
	Batch         int
	KeepSucceeded time.Duration // 1h
	KeepDead      time.Duration // 14d: the DLQ retry window
	Log           *slog.Logger
}

func (a *Archiver) Tick(ctx context.Context) (int, error) {
	now := time.Now().UTC()
	jobs, err := a.Jobs.ListArchivable(ctx, now.Add(-a.KeepSucceeded), now.Add(-a.KeepDead), a.Batch)
	if err != nil || len(jobs) == 0 {
		return 0, err
	}
	// History first, delete second: a crash in between leaves a duplicate,
	// never a lost job.
	if err := a.History.Archive(ctx, jobs); err != nil {
		return 0, err
	}
	return len(jobs), a.Jobs.DeleteArchived(ctx, jobs)
}

func (a *Archiver) Run(ctx context.Context) {
	every(ctx, a.Interval, func() bool {
		n, err := a.Tick(ctx)
		if err != nil {
			a.Log.Error("archive", "err", err)
			return false
		}
		if n > 0 {
			a.Log.Info("archived", "jobs", n)
		}
		return n == a.Batch
	})
}

// ----------------------------------------------------------------- job service

// JobService is the read side: status, history, DLQ, manual retry.
type JobService struct {
	Jobs    JobStore
	History HistoryStore
}

func (s *JobService) Get(ctx context.Context, tenantID, id string) (*domain.Job, error) {
	j, err := s.Jobs.GetJob(ctx, tenantID, id)
	if errors.Is(err, domain.ErrNotFound) {
		return s.History.GetJob(ctx, tenantID, id)
	}
	return j, err
}

// TaskHistory merges the hot table and the archive, deduping by job id (the
// archiver may leave a job in both for a moment).
func (s *JobService) TaskHistory(ctx context.Context, tenantID, taskID string, limit int) ([]domain.Job, error) {
	hot, err := s.Jobs.ListJobsForTask(ctx, tenantID, taskID, limit)
	if err != nil {
		return nil, err
	}
	old, err := s.History.ListForTask(ctx, tenantID, taskID, limit)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []domain.Job
	for _, j := range append(hot, old...) {
		if !seen[j.ID] {
			seen[j.ID] = true
			out = append(out, j)
		}
	}
	slices.SortFunc(out, func(a, b domain.Job) int { return b.ScheduledAt.Compare(a.ScheduledAt) })
	return out[:min(limit, len(out))], nil
}

func (s *JobService) DLQ(ctx context.Context, tenantID string) ([]domain.Job, error) {
	return s.Jobs.ListDead(ctx, tenantID, 500)
}

// Retry sends a dead job back to pending with one more attempt.
func (s *JobService) Retry(ctx context.Context, tenantID, id string) (*domain.Job, error) {
	if err := s.Jobs.RetryDead(ctx, tenantID, id, time.Now().UTC()); err != nil {
		return nil, err
	}
	return s.Jobs.GetJob(ctx, tenantID, id)
}
