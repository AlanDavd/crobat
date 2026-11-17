package taskservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"crobat/internal/app"
	"crobat/internal/domain"
)

// store is an in-memory app.TaskStore. Unused methods panic via the nil embed.
type store struct {
	app.TaskStore
	tasks map[string]domain.Task
	jobs  []domain.Job
}

func newStore() *store { return &store{tasks: map[string]domain.Task{}} }

func (s *store) CreateTask(_ context.Context, t *domain.Task, j *domain.Job) (*domain.Task, error) {
	s.tasks[t.ID] = *t
	if j != nil {
		s.jobs = append(s.jobs, *j)
	}
	return t, nil
}

func (s *store) GetTask(_ context.Context, tenant, id string) (*domain.Task, error) {
	t, ok := s.tasks[id]
	if !ok || t.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	return &t, nil
}

func (s *store) UpdateTask(_ context.Context, t *domain.Task) error {
	s.tasks[t.ID] = *t
	return nil
}

var ctx = context.Background()

func task(cron string) domain.Task {
	return domain.Task{Name: "t", Cron: cron, Request: domain.Request{URL: "https://example.com/hook",
		Headers: map[string]string{"Authorization": "secret"}}}
}

func TestCreateNowWritesTheFirstJobWithTheTask(t *testing.T) {
	st := newStore()
	got, err := (&Service{Tasks: st}).Create(ctx, "acme", "key-1", task(""))
	if err != nil {
		t.Fatal(err)
	}
	if got.NextRunAt != nil || got.Status != domain.TaskActive || got.TenantID != "acme" || got.IdempotencyKey != "key-1" {
		t.Fatalf("unexpected task: %+v", got)
	}
	if len(st.jobs) != 1 {
		t.Fatalf("want 1 job written with the task, got %d", len(st.jobs))
	}
	j := st.jobs[0]
	if j.TaskID != got.ID || j.Status != domain.JobPending || j.Request.Headers["Authorization"] != "secret" ||
		time.Since(j.ScheduledAt) > time.Second {
		t.Fatalf("job must copy the task and be due now: %+v", j)
	}
}

func TestCreateCronOnlySetsNextRun(t *testing.T) {
	st := newStore()
	got, err := (&Service{Tasks: st}).Create(ctx, "acme", "", task("0 9 * * *"))
	if err != nil {
		t.Fatal(err)
	}
	if len(st.jobs) != 0 {
		t.Fatal("cron tasks are planned by the scheduler, not on create")
	}
	if got.NextRunAt == nil || !got.NextRunAt.After(time.Now()) || got.NextRunAt.Hour() != 9 || got.NextRunAt.Minute() != 0 {
		t.Fatalf("next run = %v", got.NextRunAt)
	}
}

func TestCreateRejectsInvalidInput(t *testing.T) {
	st := newStore()
	in := task("")
	in.Request.URL = "file:///etc/passwd"
	if _, err := (&Service{Tasks: st}).Create(ctx, "acme", "", in); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if len(st.tasks) != 0 || len(st.jobs) != 0 {
		t.Fatal("nothing may be stored for invalid input")
	}
}

func TestUpdateRecomputesNextRunOnlyWhenScheduleChanges(t *testing.T) {
	st := newStore()
	svc := &Service{Tasks: st}
	created, _ := svc.Create(ctx, "acme", "", task("0 9 * * *"))
	sentinel := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	tk := st.tasks[created.ID]
	tk.NextRunAt = &sentinel
	st.tasks[created.ID] = tk

	in := task("0 9 * * *")
	in.Name = "renamed"
	got, err := svc.Update(ctx, "acme", created.ID, in)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "renamed" || !got.NextRunAt.Equal(sentinel) {
		t.Fatalf("a rename must not move next_run_at: %v", got.NextRunAt)
	}
	got, _ = svc.Update(ctx, "acme", created.ID, task("30 18 * * *"))
	if got.NextRunAt.Equal(sentinel) || got.NextRunAt.Hour() != 18 {
		t.Fatalf("a schedule change must recompute next_run_at: %v", got.NextRunAt)
	}
}

func TestPauseResumeDelete(t *testing.T) {
	st := newStore()
	svc := &Service{Tasks: st}
	created, _ := svc.Create(ctx, "acme", "", task("* * * * *"))

	if p, _ := svc.Pause(ctx, "acme", created.ID); p.Status != domain.TaskPaused {
		t.Fatalf("pause: %s", p.Status)
	}
	r, _ := svc.Resume(ctx, "acme", created.ID)
	if r.Status != domain.TaskActive || r.NextRunAt == nil || !r.NextRunAt.After(time.Now()) {
		t.Fatalf("resume must restart from now: %+v", r)
	}
	if err := svc.Delete(ctx, "acme", created.ID); err != nil {
		t.Fatal(err)
	}
	if d := st.tasks[created.ID]; d.Status != domain.TaskDeleted || d.NextRunAt != nil {
		t.Fatalf("delete must be soft and stop planning: %+v", d)
	}
	if _, err := svc.Pause(ctx, "acme", created.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("deleted tasks act as not found, got %v", err)
	}
}

func TestOtherTenantsCannotTouchATask(t *testing.T) {
	st := newStore()
	svc := &Service{Tasks: st}
	created, _ := svc.Create(ctx, "acme", "", task("* * * * *"))
	if _, err := svc.Get(ctx, "evil", created.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("get: %v", err)
	}
	if _, err := svc.Pause(ctx, "evil", created.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("pause: %v", err)
	}
}
