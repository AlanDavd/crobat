// Package scheduler turns due tasks into pending jobs ahead of time
// (crobat serve scheduler).
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"crobat/internal/app"
	"crobat/internal/domain"
)

// Service turns due tasks into pending jobs ahead of time.
type Service struct {
	Tasks     app.TaskStore
	Interval  time.Duration // 5s
	Lookahead time.Duration // 30s
	Batch     int
	Log       *slog.Logger
}

// maxCatchUp bounds the jobs one task can create per tick after an outage;
// the rest are created on the following ticks.
const maxCatchUp = 1000

func (s *Service) Tick(ctx context.Context) (int, error) {
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

func (s *Service) Run(ctx context.Context) {
	app.Every(ctx, s.Interval, func() bool {
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
