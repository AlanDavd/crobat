package jobservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"crobat/internal/app"
	"crobat/internal/domain"
)

type hot struct {
	app.JobStore
	jobs     map[string]domain.Job
	getErr   error
	retryErr error
}

func (h *hot) GetJob(_ context.Context, _, id string) (*domain.Job, error) {
	if h.getErr != nil {
		return nil, h.getErr
	}
	j, ok := h.jobs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return &j, nil
}

func (h *hot) ListJobsForTask(context.Context, string, string, int) ([]domain.Job, error) {
	var out []domain.Job
	for _, j := range h.jobs {
		out = append(out, j)
	}
	return out, nil
}

func (h *hot) RetryDead(_ context.Context, _, id string, _ time.Time) error {
	if h.retryErr != nil {
		return h.retryErr
	}
	j := h.jobs[id]
	j.Status = domain.JobPending
	h.jobs[id] = j
	return nil
}

type archive struct {
	app.HistoryStore
	jobs []domain.Job
}

func (a *archive) GetJob(_ context.Context, _, id string) (*domain.Job, error) {
	for _, j := range a.jobs {
		if j.ID == id {
			return &j, nil
		}
	}
	return nil, domain.ErrNotFound
}

func (a *archive) ListForTask(context.Context, string, string, int) ([]domain.Job, error) {
	return a.jobs, nil
}

var ctx = context.Background()

func at(min int) time.Time { return time.Date(2026, 1, 1, 9, min, 0, 0, time.UTC) }

func TestGetFallsBackToTheArchive(t *testing.T) {
	s := &Service{Jobs: &hot{jobs: map[string]domain.Job{}}, History: &archive{jobs: []domain.Job{{ID: "old", Archived: true}}}}
	j, err := s.Get(ctx, "acme", "old")
	if err != nil || !j.Archived {
		t.Fatalf("j=%+v err=%v", j, err)
	}
	if _, err := s.Get(ctx, "acme", "nope"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestGetDoesNotHideDatabaseErrors(t *testing.T) {
	boom := errors.New("mysql down")
	s := &Service{Jobs: &hot{getErr: boom}, History: &archive{jobs: []domain.Job{{ID: "x"}}}}
	if _, err := s.Get(ctx, "acme", "x"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the MySQL error, not a stale archive read", err)
	}
}

func TestTaskHistoryMergesDedupesSortsAndLimits(t *testing.T) {
	s := &Service{
		Jobs: &hot{jobs: map[string]domain.Job{
			"b": {ID: "b", ScheduledAt: at(2), Status: domain.JobSucceeded},
			"d": {ID: "d", ScheduledAt: at(4), Status: domain.JobPending},
		}},
		History: &archive{jobs: []domain.Job{
			{ID: "b", ScheduledAt: at(2), Status: domain.JobSucceeded, Archived: true}, // mid-archive duplicate
			{ID: "a", ScheduledAt: at(1), Archived: true},
			{ID: "c", ScheduledAt: at(3), Archived: true},
		}},
	}
	got, err := s.TaskHistory(ctx, "acme", "t1", 3)
	if err != nil {
		t.Fatal(err)
	}
	var ids string
	for _, j := range got {
		ids += j.ID
	}
	if ids != "dcb" {
		t.Fatalf("got %q, want newest first, deduped, limited: dcb", ids)
	}
	if got[2].Archived {
		t.Fatal("the hot (MySQL) copy must win over the archived duplicate")
	}
}

func TestRetry(t *testing.T) {
	h := &hot{jobs: map[string]domain.Job{"j": {ID: "j", Status: domain.JobDead}}}
	s := &Service{Jobs: h, History: &archive{}}
	j, err := s.Retry(ctx, "acme", "j")
	if err != nil || j.Status != domain.JobPending {
		t.Fatalf("j=%+v err=%v", j, err)
	}
	h.retryErr = domain.ErrNotFound
	if _, err := s.Retry(ctx, "acme", "j"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}
