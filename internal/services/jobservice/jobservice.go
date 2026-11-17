// Package jobservice is the read side: job status, task history, the DLQ
// and manual retries. Driven by the REST adapter (crobat serve job-api).
package jobservice

import (
	"context"
	"errors"
	"slices"
	"time"

	"crobat/internal/app"
	"crobat/internal/domain"
)

// Service is the read side: status, history, DLQ, manual retry.
type Service struct {
	Jobs    app.JobStore
	History app.HistoryStore
}

func (s *Service) Get(ctx context.Context, tenantID, id string) (*domain.Job, error) {
	j, err := s.Jobs.GetJob(ctx, tenantID, id)
	if errors.Is(err, domain.ErrNotFound) {
		return s.History.GetJob(ctx, tenantID, id)
	}
	return j, err
}

// TaskHistory merges the hot table and the archive, deduping by job id (the
// archiver may leave a job in both for a moment).
func (s *Service) TaskHistory(ctx context.Context, tenantID, taskID string, limit int) ([]domain.Job, error) {
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

func (s *Service) DLQ(ctx context.Context, tenantID string) ([]domain.Job, error) {
	return s.Jobs.ListDead(ctx, tenantID, 500)
}

// Retry sends a dead job back to pending with one more attempt.
func (s *Service) Retry(ctx context.Context, tenantID, id string) (*domain.Job, error) {
	if err := s.Jobs.RetryDead(ctx, tenantID, id, time.Now().UTC()); err != nil {
		return nil, err
	}
	return s.Jobs.GetJob(ctx, tenantID, id)
}
