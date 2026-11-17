package app

import (
	"context"
	"testing"
	"time"
)

func TestEveryRunsAgainWhileWorkIsWaitingThenStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	done := make(chan struct{})
	go func() {
		Every(ctx, time.Hour, func() bool {
			calls++
			if calls == 3 {
				cancel() // the third call reports no more work and cancels
				return false
			}
			return true // more work: run again without waiting an hour
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Every did not return after cancel")
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}
