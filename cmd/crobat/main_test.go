package main

import (
	"encoding/json"
	"testing"
)

func TestTaskBodyFromFlags(t *testing.T) {
	b, idem, err := taskBody([]string{
		"-name", "daily", "-url", "https://e.com/run", "-cron", "0 9 * * *", "-tz", "Europe/Madrid",
		"-priority", "high", "-header", "Authorization: Bearer x:y", "-header", "X-A:1",
		"-max-attempts", "5", "-idempotency-key", "k1",
	})
	if err != nil {
		t.Fatal(err)
	}
	js, _ := json.Marshal(b)
	var got struct {
		Name, Cron, Timezone, Priority string
		At                             *string
		Request                        struct{ Headers map[string]string }
		Retry                          struct {
			MaxAttempts int `json:"max_attempts"`
		}
	}
	_ = json.Unmarshal(js, &got)
	if got.Name != "daily" || got.Cron != "0 9 * * *" || got.Timezone != "Europe/Madrid" || got.Priority != "high" ||
		got.At != nil || got.Retry.MaxAttempts != 5 || idem != "k1" {
		t.Fatalf("body = %s idem = %q", js, idem)
	}
	if got.Request.Headers["Authorization"] != "Bearer x:y" || got.Request.Headers["X-A"] != "1" {
		t.Fatalf("headers split on the first colon only: %v", got.Request.Headers)
	}
}

func TestTaskBodyRejectsMalformedHeaders(t *testing.T) {
	if _, _, err := taskBody([]string{"-header", "no-colon"}); err == nil {
		t.Fatal("want an error")
	}
}
