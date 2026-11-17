package domain

import (
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"
)

func valid() Task {
	return Task{Name: "n", Request: Request{URL: "https://e.com/hook"}}
}

func TestNormalizeFillsDefaults(t *testing.T) {
	tk := valid()
	if err := tk.Normalize(); err != nil {
		t.Fatal(err)
	}
	if tk.Request.Method != "POST" || tk.Timezone != "UTC" || tk.TimeoutMS != 10_000 ||
		tk.Retry.MaxAttempts != 3 || tk.Retry.BackoffMS != 1_000 {
		t.Fatalf("defaults: %+v", tk)
	}
}

func TestNormalizeRejectsBadInput(t *testing.T) {
	at := time.Now()
	for name, mutate := range map[string]func(*Task){
		"empty name":      func(t *Task) { t.Name = "  " },
		"cron and at":     func(t *Task) { t.Cron, t.At = "* * * * *", &at },
		"bad cron":        func(t *Task) { t.Cron = "61 * * * *" },
		"bad timezone":    func(t *Task) { t.Timezone = "Mars/Olympus" },
		"bad priority":    func(t *Task) { t.Priority = 9 },
		"bad method":      func(t *Task) { t.Request.Method = "CONNECT" },
		"relative url":    func(t *Task) { t.Request.URL = "/hook" },
		"file url":        func(t *Task) { t.Request.URL = "file:///etc/passwd" },
		"timeout too low": func(t *Task) { t.TimeoutMS = 5 },
		"too many tries":  func(t *Task) { t.Retry.MaxAttempts = 100 },
		"backoff too low": func(t *Task) { t.Retry.BackoffMS = 1 },
	} {
		tk := valid()
		mutate(&tk)
		if err := tk.Normalize(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
}

func TestFirstRun(t *testing.T) {
	now := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	if got := (&Task{}).FirstRun(now); !got.Equal(now) {
		t.Fatalf("now: %v", got)
	}
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.FixedZone("x", 3600))
	if got := (&Task{At: &at}).FirstRun(now); !got.Equal(at) || got.Location() != time.UTC {
		t.Fatalf("at: %v", got)
	}
	if got := (&Task{Cron: "0 9 * * *", Timezone: "UTC"}).FirstRun(now); got.Hour() != 9 || got.Day() != 1 {
		t.Fatalf("cron: %v", got)
	}
}

func TestNewJobCopiesTheTask(t *testing.T) {
	tk := Task{ID: "t", TenantID: "acme", Priority: Critical, TimeoutMS: 500, Retry: RetryPolicy{MaxAttempts: 7},
		Request: Request{URL: "https://e.com", Headers: map[string]string{"A": "b"}}}
	due := time.Now()
	j := tk.NewJob(due)
	tk.Request.URL = "https://changed.com" // editing the task later...
	if j.Request.URL != "https://e.com" || j.Priority != Critical || j.TimeoutMS != 500 || j.Retry.MaxAttempts != 7 ||
		!j.NextAttemptAt.Equal(due) || j.Status != JobPending || j.TenantID != "acme" {
		t.Fatalf("...must not change the planned job: %+v", j)
	}
}

func TestPriorityJSON(t *testing.T) {
	b, _ := json.Marshal(struct{ P Priority }{High})
	if string(b) != `{"P":"high"}` {
		t.Fatalf("marshal: %s", b)
	}
	var v struct{ P Priority }
	if err := json.Unmarshal([]byte(`{"P":"low"}`), &v); err != nil || v.P != Low {
		t.Fatalf("unmarshal: %v %v", v.P, err)
	}
	if err := json.Unmarshal([]byte(`{"P":"urgent"}`), &v); err == nil {
		t.Fatal("unknown priority accepted")
	}
}

func TestSecretsAreNeverSerialized(t *testing.T) {
	b, _ := json.Marshal(Job{Request: Request{Headers: map[string]string{"Authorization": "TOPSECRET"}, Body: "BODYSECRET"}})
	if regexp.MustCompile(`TOPSECRET|BODYSECRET`).Match(b) {
		t.Fatalf("leaked: %s", b)
	}
}

func TestNewIDIsAUUIDv4(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for range 1000 {
		id := NewID()
		if !re.MatchString(id) || seen[id] {
			t.Fatalf("bad or duplicate id %q", id)
		}
		seen[id] = true
	}
}
