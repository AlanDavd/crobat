// Package executor claims jobs from the jobs table, fires each on its own
// timer, calls the endpoint and saves the result (crobat serve executor).
package executor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"crobat/internal/app"
	"crobat/internal/domain"
)

// Service claims jobs, fires each on its own timer and saves the result.
type Service struct {
	Jobs     app.JobStore
	Caller   app.Caller
	WorkerID string
	Workers  int
	Poll     time.Duration
	Log      *slog.Logger
}

func (e *Service) Run(ctx context.Context) {
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

func (e *Service) fire(ctx context.Context, j domain.Job) {
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
