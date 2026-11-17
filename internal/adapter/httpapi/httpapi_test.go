package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"crobat/internal/app"
	"crobat/internal/domain"
	"crobat/internal/services/jobservice"
	"crobat/internal/services/taskservice"
)

var secret = []byte("test-secret")

func TestTokens(t *testing.T) {
	req := func(auth string) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		return r
	}
	good := SignToken(secret, "acme", time.Hour)
	if tenant, ok := tenantFrom(secret, req("Bearer "+good)); !ok || tenant != "acme" {
		t.Fatalf("valid token rejected: %q %v", tenant, ok)
	}
	parts := strings.Split(good, ".")
	none := b64.EncodeToString([]byte(`{"alg":"none"}`)) + "." + parts[1] + "."
	forged := parts[0] + "." + b64.EncodeToString([]byte(`{"sub":"other","exp":9999999999}`)) + "." + parts[2]
	for name, auth := range map[string]string{
		"missing":      "",
		"not bearer":   "Basic " + base64.StdEncoding.EncodeToString([]byte("a:b")),
		"garbage":      "Bearer abc",
		"wrong secret": "Bearer " + SignToken([]byte("other"), "acme", time.Hour),
		"expired":      "Bearer " + SignToken(secret, "acme", -time.Minute),
		"alg none":     "Bearer " + none,
		"forged sub":   "Bearer " + forged,
		"empty sub":    "Bearer " + SignToken(secret, "", time.Hour),
	} {
		if _, ok := tenantFrom(secret, req(auth)); ok {
			t.Errorf("%s: accepted", name)
		}
	}
}

// tasks is an in-memory TaskStore that can be told to fail.
type tasks struct {
	app.TaskStore
	m   map[string]domain.Task
	err error
}

func (s *tasks) CreateTask(_ context.Context, t *domain.Task, _ *domain.Job) (*domain.Task, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.m[t.ID] = *t
	return t, nil
}

func (s *tasks) GetTask(_ context.Context, tenant, id string) (*domain.Task, error) {
	t, ok := s.m[id]
	if !ok || t.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	return &t, nil
}

func (s *tasks) UpdateTask(_ context.Context, t *domain.Task) error { s.m[t.ID] = *t; return nil }

func (s *tasks) ListTasks(_ context.Context, tenant string, _ int) ([]domain.Task, error) {
	var out []domain.Task
	for _, t := range s.m {
		if t.TenantID == tenant {
			out = append(out, t)
		}
	}
	return out, nil
}

func do(t *testing.T, h http.Handler, method, path, tenant, body string) (int, string) {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if tenant != "" {
		r.Header.Set("Authorization", "Bearer "+SignToken(secret, tenant, time.Hour))
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	b, _ := io.ReadAll(w.Result().Body)
	return w.Code, string(b)
}

const validTask = `{"name":"n","request":{"url":"https://example.com/hook","headers":{"Authorization":"Bearer TOPSECRET"},"body":"BODYSECRET"}}`

func newTaskAPI() (http.Handler, *tasks) {
	st := &tasks{m: map[string]domain.Task{}}
	return TaskAPI(&taskservice.Service{Tasks: st}, secret, slog.New(slog.DiscardHandler)), st
}

func TestTaskAPI(t *testing.T) {
	h, _ := newTaskAPI()

	if code, _ := do(t, h, "GET", "/healthz", "", ""); code != 200 {
		t.Fatalf("healthz = %d", code)
	}
	if code, _ := do(t, h, "GET", "/tasks", "", ""); code != 401 {
		t.Fatalf("no token = %d", code)
	}

	code, body := do(t, h, "POST", "/tasks", "acme", validTask)
	if code != 201 {
		t.Fatalf("create = %d %s", code, body)
	}
	if strings.Contains(body, "TOPSECRET") || strings.Contains(body, "BODYSECRET") {
		t.Fatalf("secrets must never be returned: %s", body)
	}
	var created domain.Task
	_ = json.Unmarshal([]byte(body), &created)
	if created.TenantID != "acme" || created.Priority != domain.Normal {
		t.Fatalf("tenant must come from the token, priority default normal: %+v", created)
	}

	if code, _ := do(t, h, "GET", "/tasks/"+created.ID, "evil", ""); code != 404 {
		t.Fatalf("other tenant = %d, want 404", code)
	}
	if code, _ := do(t, h, "POST", "/tasks/"+created.ID+"/pause", "acme", ""); code != 200 {
		t.Fatalf("pause = %d", code)
	}
	if code, _ := do(t, h, "DELETE", "/tasks/"+created.ID, "acme", ""); code != 204 {
		t.Fatalf("delete = %d", code)
	}
}

func TestTaskAPIRejectsBadInput(t *testing.T) {
	h, _ := newTaskAPI()
	for name, body := range map[string]string{
		"not json":      `{`,
		"unknown field": `{"name":"n","tenant_id":"evil","request":{"url":"https://e.com"}}`,
		"bad priority":  `{"name":"n","priority":"urgent","request":{"url":"https://e.com"}}`,
		"bad url":       `{"name":"n","request":{"url":"gopher://e.com"}}`,
	} {
		if code, _ := do(t, h, "POST", "/tasks", "acme", body); code != 400 {
			t.Errorf("%s: %d, want 400", name, code)
		}
	}
}

func TestInternalErrorsAreNotLeaked(t *testing.T) {
	h, st := newTaskAPI()
	st.err = errors.New("dial tcp 10.0.0.5:3306: password=hunter2")
	code, body := do(t, h, "POST", "/tasks", "acme", validTask)
	if code != 500 || strings.Contains(body, "hunter2") || strings.Contains(body, "10.0.0.5") {
		t.Fatalf("%d %s", code, body)
	}
}

type jobs struct {
	app.JobStore
	m map[string]domain.Job
}

func (s *jobs) GetJob(_ context.Context, tenant, id string) (*domain.Job, error) {
	j, ok := s.m[id]
	if !ok || j.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	return &j, nil
}

func (s *jobs) ListDead(context.Context, string, int) ([]domain.Job, error) { return nil, nil }

type history struct{ app.HistoryStore }

func (history) GetJob(context.Context, string, string) (*domain.Job, error) {
	return nil, domain.ErrNotFound
}

func TestJobAPI(t *testing.T) {
	st := &jobs{m: map[string]domain.Job{"j1": {ID: "j1", TenantID: "acme", Status: domain.JobSucceeded,
		Request: domain.Request{URL: "https://e.com", Headers: map[string]string{"Authorization": "TOPSECRET"}}}}}
	h := JobAPI(&jobservice.Service{Jobs: st, History: history{}}, secret, slog.New(slog.DiscardHandler))

	code, body := do(t, h, "GET", "/jobs/j1", "acme", "")
	if code != 200 || !strings.Contains(body, `"succeeded"`) || strings.Contains(body, "TOPSECRET") {
		t.Fatalf("%d %s", code, body)
	}
	if code, _ := do(t, h, "GET", "/jobs/j1", "evil", ""); code != 404 {
		t.Fatalf("other tenant = %d", code)
	}
	if code, _ := do(t, h, "GET", "/dlq", "acme", ""); code != 200 {
		t.Fatalf("dlq = %d", code)
	}
}
