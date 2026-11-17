package archiver

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"crobat/internal/app"
	"crobat/internal/domain"
)

// events records the order of calls across both stores.
type events []string

type jobs struct {
	app.JobStore
	ev                          *events
	list                        []domain.Job
	succeededBefore, deadBefore time.Time
	deleted                     []domain.Job
}

func (j *jobs) ListArchivable(_ context.Context, succeededBefore, deadBefore time.Time, _ int) ([]domain.Job, error) {
	j.succeededBefore, j.deadBefore = succeededBefore, deadBefore
	return j.list, nil
}

func (j *jobs) DeleteArchived(_ context.Context, js []domain.Job) error {
	*j.ev = append(*j.ev, "delete")
	j.deleted = js
	return nil
}

type history struct {
	app.HistoryStore
	ev  *events
	err error
}

func (h *history) Archive(context.Context, []domain.Job) error {
	*h.ev = append(*h.ev, "archive")
	return h.err
}

func service(j *jobs, h *history) *Service {
	return &Service{Jobs: j, History: h, Batch: 500, KeepSucceeded: time.Hour, KeepDead: 14 * 24 * time.Hour,
		Log: slog.New(slog.DiscardHandler)}
}

func TestArchivesToHistoryBeforeDeleting(t *testing.T) {
	ev := &events{}
	j := &jobs{ev: ev, list: []domain.Job{{ID: "a"}, {ID: "b"}}}
	n, err := service(j, &history{ev: ev}).Tick(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if len(*ev) != 2 || (*ev)[0] != "archive" || (*ev)[1] != "delete" {
		t.Fatalf("order = %v, history must be written first", *ev)
	}
	if len(j.deleted) != 2 {
		t.Fatalf("deleted %d", len(j.deleted))
	}
}

func TestNeverDeletesWhenHistoryWriteFails(t *testing.T) {
	ev := &events{}
	j := &jobs{ev: ev, list: []domain.Job{{ID: "a"}}}
	if _, err := service(j, &history{ev: ev, err: errors.New("cassandra down")}).Tick(context.Background()); err == nil {
		t.Fatal("want the archive error")
	}
	if j.deleted != nil {
		t.Fatal("deleting without a history copy would lose the job")
	}
}

func TestUsesTheRetentionWindows(t *testing.T) {
	ev := &events{}
	j := &jobs{ev: ev}
	if _, err := service(j, &history{ev: ev}).Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(j.succeededBefore); d < time.Hour || d > time.Hour+time.Second {
		t.Fatalf("succeeded cutoff %v ago", d)
	}
	if d := time.Since(j.deadBefore); d < 336*time.Hour || d > 336*time.Hour+time.Second {
		t.Fatalf("dead cutoff %v ago (the DLQ window)", d)
	}
	if len(*ev) != 0 {
		t.Fatalf("nothing to archive must touch nothing: %v", *ev)
	}
}
