// Package taskservice is the write API for tasks: create, edit, pause,
// resume and delete. Driven by the REST adapter (crobat serve task-api).
package taskservice

import (
	"context"
	"time"

	"crobat/internal/app"
	"crobat/internal/domain"
)

// Service is the write API for tasks.
type Service struct{ Tasks app.TaskStore }

func (s *Service) Create(ctx context.Context, tenantID, idemKey string, t domain.Task) (*domain.Task, error) {
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

func (s *Service) Get(ctx context.Context, tenantID, id string) (*domain.Task, error) {
	return s.Tasks.GetTask(ctx, tenantID, id)
}

func (s *Service) List(ctx context.Context, tenantID string) ([]domain.Task, error) {
	return s.Tasks.ListTasks(ctx, tenantID, 500)
}

// Update replaces the editable fields. Existing jobs are untouched.
func (s *Service) Update(ctx context.Context, tenantID, id string, in domain.Task) (*domain.Task, error) {
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

func (s *Service) Pause(ctx context.Context, tenantID, id string) (*domain.Task, error) {
	return s.setStatus(ctx, tenantID, id, domain.TaskPaused)
}

func (s *Service) Resume(ctx context.Context, tenantID, id string) (*domain.Task, error) {
	return s.setStatus(ctx, tenantID, id, domain.TaskActive)
}

func (s *Service) Delete(ctx context.Context, tenantID, id string) error {
	_, err := s.setStatus(ctx, tenantID, id, domain.TaskDeleted)
	return err
}

func (s *Service) setStatus(ctx context.Context, tenantID, id string, st domain.TaskStatus) (*domain.Task, error) {
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

func (s *Service) save(ctx context.Context, t *domain.Task) error {
	t.UpdatedAt = time.Now().UTC()
	return s.Tasks.UpdateTask(ctx, t)
}

func timeEq(a, b *time.Time) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && a.Equal(*b))
}
