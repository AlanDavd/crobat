// Package app holds the six services. Each depends only on the ports below;
// adapters (MySQL, Cassandra, HTTP) implement or drive them.
package app

import (
	"context"
	"time"

	"crobat/internal/domain"
)

// TaskStore is the tasks table.
type TaskStore interface {
	// CreateTask saves t and, if non-nil, its first job in one transaction.
	// On a repeated (tenant, idempotency key) it returns the existing task.
	CreateTask(ctx context.Context, t *domain.Task, firstJob *domain.Job) (*domain.Task, error)
	GetTask(ctx context.Context, tenantID, id string) (*domain.Task, error)
	ListTasks(ctx context.Context, tenantID string, limit int) ([]domain.Task, error)
	UpdateTask(ctx context.Context, t *domain.Task) error
	// ScheduleDue locks active tasks with next_run_at <= horizon, asks plan
	// for their jobs (plan advances t.NextRunAt), inserts the jobs and saves
	// next_run_at in one transaction. Returns the number of tasks handled.
	ScheduleDue(ctx context.Context, horizon time.Time, limit int, plan func(*domain.Task) []domain.Job) (int, error)
}

// JobStore is the jobs table: the hot table and the queue.
type JobStore interface {
	// Claim locks up to n pending jobs due before horizon (SKIP LOCKED),
	// marks them running for workerID with attempt+1 and a lease, commits.
	Claim(ctx context.Context, workerID string, n int, horizon, now time.Time) ([]domain.Job, error)
	// SaveResult writes j's new state only if it is still running for
	// workerID at j.Attempt (the fencing token). False means fenced off.
	SaveResult(ctx context.Context, j *domain.Job, workerID string) (bool, error)
	ReleaseExpired(ctx context.Context, now time.Time, limit int) (int64, error)
	ListArchivable(ctx context.Context, succeededBefore, deadBefore time.Time, limit int) ([]domain.Job, error)
	DeleteArchived(ctx context.Context, jobs []domain.Job) error
	GetJob(ctx context.Context, tenantID, id string) (*domain.Job, error)
	ListJobsForTask(ctx context.Context, tenantID, taskID string, limit int) ([]domain.Job, error)
	ListDead(ctx context.Context, tenantID string, limit int) ([]domain.Job, error)
	RetryDead(ctx context.Context, tenantID, id string, now time.Time) error
}

// HistoryStore is the append-only archive (Cassandra).
type HistoryStore interface {
	Archive(ctx context.Context, jobs []domain.Job) error
	GetJob(ctx context.Context, tenantID, id string) (*domain.Job, error)
	ListForTask(ctx context.Context, tenantID, taskID string, limit int) ([]domain.Job, error)
}

// Caller performs the job's HTTP request.
type Caller interface {
	Call(ctx context.Context, j domain.Job) domain.Result
}

// every runs fn now and then on each tick until ctx ends. fn returns true to
// run again immediately (more work is waiting).
func every(ctx context.Context, d time.Duration, fn func() bool) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		for fn() && ctx.Err() == nil {
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
