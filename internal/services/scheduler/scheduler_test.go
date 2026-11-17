package scheduler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"crobat/internal/app"
	"crobat/internal/domain"
)

// store hands the scheduler a fixed set of tasks and records the plan.
type store struct {
	app.TaskStore
	tasks   []*domain.Task
	horizon time.Time
	jobs    []domain.Job
}

func (s *store) ScheduleDue(_ context.Context, horizon time.Time, _ int, plan func(*domain.Task) []domain.Job) (int, error) {
	s.horizon = horizon
	for _, t := range s.tasks {
		s.jobs = append(s.jobs, plan(t)...)
	}
	return len(s.tasks), nil
}

func tick(t *testing.T, tasks ...*domain.Task) *store {
	t.Helper()
	st := &store{tasks: tasks}
	svc := &Service{Tasks: st, Lookahead: 30 * time.Second, Batch: 500, Log: slog.New(slog.DiscardHandler)}
	if _, err := svc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	return st
}

func ptr(t time.Time) *time.Time { return &t }

func TestTickCatchesUpMissedRunsFromThePreviousRun(t *testing.T) {
	due := time.Now().UTC().Truncate(time.Minute).Add(-5 * time.Minute)
	task := &domain.Task{ID: "t1", Cron: "* * * * *", Timezone: "UTC", NextRunAt: ptr(due)}
	st := tick(t, task)

	if len(st.jobs) < 6 {
		t.Fatalf("want every missed minute planned, got %d jobs", len(st.jobs))
	}
	for i, j := range st.jobs {
		if want := due.Add(time.Duration(i) * time.Minute); !j.ScheduledAt.Equal(want) {
			t.Fatalf("job %d at %v, want %v", i, j.ScheduledAt, want)
		}
		if j.ScheduledAt.After(st.horizon) {
			t.Fatalf("job %d planned past the horizon", i)
		}
	}
	if !task.NextRunAt.After(st.horizon) {
		t.Fatalf("next_run_at %v must move past the horizon %v", task.NextRunAt, st.horizon)
	}
}

func TestTickPlansOneShotTasksOnce(t *testing.T) {
	at := time.Now().UTC().Add(10 * time.Second)
	task := &domain.Task{ID: "t1", At: &at, NextRunAt: ptr(at), Priority: domain.High, TimeoutMS: 1234}
	st := tick(t, task)
	if len(st.jobs) != 1 || !st.jobs[0].ScheduledAt.Equal(at) || st.jobs[0].Priority != domain.High || st.jobs[0].TimeoutMS != 1234 {
		t.Fatalf("jobs = %+v", st.jobs)
	}
	if task.NextRunAt != nil {
		t.Fatal("a one-shot task must not be planned again")
	}
}

func TestTickBoundsCatchUpPerTask(t *testing.T) {
	task := &domain.Task{ID: "t1", Cron: "* * * * *", Timezone: "UTC", NextRunAt: ptr(time.Now().UTC().Add(-72 * time.Hour))}
	st := tick(t, task)
	if len(st.jobs) != maxCatchUp {
		t.Fatalf("want %d jobs per tick, got %d", maxCatchUp, len(st.jobs))
	}
	if task.NextRunAt.After(st.horizon) {
		t.Fatal("the rest of the backlog must stay due for the next tick")
	}
}

func TestTickIgnoresRunsBeyondTheHorizon(t *testing.T) {
	task := &domain.Task{ID: "t1", Cron: "0 9 * * *", Timezone: "UTC", NextRunAt: ptr(time.Now().Add(time.Hour))}
	if st := tick(t, task); len(st.jobs) != 0 {
		t.Fatalf("planned %d jobs too early", len(st.jobs))
	}
}
