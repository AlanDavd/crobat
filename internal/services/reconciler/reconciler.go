// Package reconciler rescues jobs whose executor died by returning
// expired leases to pending or dead (crobat serve reconciler).
package reconciler

import (
	"context"
	"log/slog"
	"time"

	"crobat/internal/app"
)

// Service returns running jobs with an expired lease to pending (or dead).
type Service struct {
	Jobs     app.JobStore
	Interval time.Duration
	Batch    int
	Log      *slog.Logger
}

func (r *Service) Run(ctx context.Context) {
	app.Every(ctx, r.Interval, func() bool {
		n, err := r.Jobs.ReleaseExpired(ctx, time.Now().UTC(), r.Batch)
		if err != nil {
			r.Log.Error("release expired", "err", err)
			return false
		}
		if n > 0 {
			r.Log.Warn("released expired leases", "jobs", n)
		}
		return n == int64(r.Batch)
	})
}
