// Package archiver moves finished jobs from MySQL to the history store
// (crobat serve archiver).
package archiver

import (
	"context"
	"log/slog"
	"time"

	"crobat/internal/app"
)

// Service copies finished jobs to history, then deletes them from MySQL.
type Service struct {
	Jobs          app.JobStore
	History       app.HistoryStore
	Interval      time.Duration
	Batch         int
	KeepSucceeded time.Duration // 1h
	KeepDead      time.Duration // 14d: the DLQ retry window
	Log           *slog.Logger
}

func (a *Service) Tick(ctx context.Context) (int, error) {
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

func (a *Service) Run(ctx context.Context) {
	app.Every(ctx, a.Interval, func() bool {
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
