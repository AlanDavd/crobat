// Package httpapi exposes the task service and the job service over HTTP.
// The tenant always comes from the JWT, never from the request body.
package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"crobat/internal/domain"
	"crobat/internal/services/jobservice"
	"crobat/internal/services/taskservice"
)

// ------------------------------------------------------------------------ JWT

var b64 = base64.RawURLEncoding

// SignToken mints an HS256 JWT with sub = tenant (used by the dev CLI).
func SignToken(secret []byte, tenant string, ttl time.Duration) string {
	head := b64.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{"sub": tenant, "exp": time.Now().Add(ttl).Unix()})
	msg := head + "." + b64.EncodeToString(claims)
	return msg + "." + b64.EncodeToString(sign(secret, msg))
}

func sign(secret []byte, msg string) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

// tenantFrom verifies an HS256 bearer token and returns its sub claim.
func tenantFrom(secret []byte, r *http.Request) (string, bool) {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	parts := strings.Split(tok, ".")
	if !ok || len(parts) != 3 {
		return "", false
	}
	var head struct{ Alg string }
	hb, err := b64.DecodeString(parts[0])
	if err != nil || json.Unmarshal(hb, &head) != nil || head.Alg != "HS256" {
		return "", false
	}
	sig, err := b64.DecodeString(parts[2])
	if err != nil || !hmac.Equal(sig, sign(secret, parts[0]+"."+parts[1])) {
		return "", false
	}
	var c struct {
		Sub string `json:"sub"`
		Exp int64  `json:"exp"`
	}
	cb, err := b64.DecodeString(parts[1])
	if err != nil || json.Unmarshal(cb, &c) != nil || c.Sub == "" || len(c.Sub) > 64 || time.Now().Unix() >= c.Exp {
		return "", false
	}
	return c.Sub, true
}

// ------------------------------------------------------------------- plumbing

type handlerFunc func(ctx context.Context, tenant string, r *http.Request) (any, error)

type api struct {
	secret []byte
	log    *slog.Logger
	mux    *http.ServeMux
}

func newAPI(secret []byte, log *slog.Logger) *api {
	a := &api{secret: secret, log: log, mux: http.NewServeMux()}
	a.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return a
}

func (a *api) handle(pattern string, status int, h handlerFunc) {
	a.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := tenantFrom(a.secret, r)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid or missing bearer token"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
		out, err := h(r.Context(), tenant, r)
		switch {
		case err == nil:
			writeJSON(w, status, out)
		case errors.Is(err, domain.ErrNotFound):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		case errors.Is(err, domain.ErrInvalid):
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		default:
			// Log the cause, return nothing internal. Never log the body: it may hold secrets.
			a.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		}
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

// taskInput is the body of POST/PUT /tasks. Unlike domain.Task, it accepts
// request headers and body (which are never returned).
type taskInput struct {
	Name     string          `json:"name"`
	Cron     string          `json:"cron"`
	At       *time.Time      `json:"at"`
	Timezone string          `json:"timezone"`
	Priority domain.Priority `json:"priority"`
	Request  struct {
		Method  string            `json:"method"`
		URL     string            `json:"url"`
		Headers map[string]string `json:"headers"`
		Body    string            `json:"body"`
	} `json:"request"`
	TimeoutMS int                `json:"timeout_ms"`
	Retry     domain.RetryPolicy `json:"retry"`
}

func decodeTask(r *http.Request) (domain.Task, error) {
	in := taskInput{Priority: domain.Normal}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return domain.Task{}, fmt.Errorf("%w: %v", domain.ErrInvalid, err)
	}
	return domain.Task{
		Name: in.Name, Cron: in.Cron, At: in.At, Timezone: in.Timezone, Priority: in.Priority,
		Request:   domain.Request{Method: in.Request.Method, URL: in.Request.URL, Headers: in.Request.Headers, Body: in.Request.Body},
		TimeoutMS: in.TimeoutMS, Retry: in.Retry,
	}, nil
}

// ------------------------------------------------------------------ task API

func TaskAPI(svc *taskservice.Service, secret []byte, log *slog.Logger) http.Handler {
	a := newAPI(secret, log)
	a.handle("POST /tasks", http.StatusCreated, func(ctx context.Context, tenant string, r *http.Request) (any, error) {
		t, err := decodeTask(r)
		if err != nil {
			return nil, err
		}
		key := r.Header.Get("Idempotency-Key")
		if len(key) > 255 {
			return nil, fmt.Errorf("%w: Idempotency-Key too long", domain.ErrInvalid)
		}
		return svc.Create(ctx, tenant, key, t)
	})
	a.handle("GET /tasks", http.StatusOK, func(ctx context.Context, tenant string, _ *http.Request) (any, error) {
		return svc.List(ctx, tenant)
	})
	a.handle("GET /tasks/{id}", http.StatusOK, func(ctx context.Context, tenant string, r *http.Request) (any, error) {
		return svc.Get(ctx, tenant, r.PathValue("id"))
	})
	a.handle("PUT /tasks/{id}", http.StatusOK, func(ctx context.Context, tenant string, r *http.Request) (any, error) {
		t, err := decodeTask(r)
		if err != nil {
			return nil, err
		}
		return svc.Update(ctx, tenant, r.PathValue("id"), t)
	})
	a.handle("POST /tasks/{id}/pause", http.StatusOK, func(ctx context.Context, tenant string, r *http.Request) (any, error) {
		return svc.Pause(ctx, tenant, r.PathValue("id"))
	})
	a.handle("POST /tasks/{id}/resume", http.StatusOK, func(ctx context.Context, tenant string, r *http.Request) (any, error) {
		return svc.Resume(ctx, tenant, r.PathValue("id"))
	})
	a.handle("DELETE /tasks/{id}", http.StatusNoContent, func(ctx context.Context, tenant string, r *http.Request) (any, error) {
		return nil, svc.Delete(ctx, tenant, r.PathValue("id"))
	})
	return a.mux
}

// ------------------------------------------------------------------- job API

func JobAPI(svc *jobservice.Service, secret []byte, log *slog.Logger) http.Handler {
	a := newAPI(secret, log)
	a.handle("GET /jobs/{id}", http.StatusOK, func(ctx context.Context, tenant string, r *http.Request) (any, error) {
		return svc.Get(ctx, tenant, r.PathValue("id"))
	})
	a.handle("POST /jobs/{id}/retry", http.StatusOK, func(ctx context.Context, tenant string, r *http.Request) (any, error) {
		return svc.Retry(ctx, tenant, r.PathValue("id"))
	})
	a.handle("GET /tasks/{id}/jobs", http.StatusOK, func(ctx context.Context, tenant string, r *http.Request) (any, error) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 500 {
			limit = 50
		}
		return svc.TaskHistory(ctx, tenant, r.PathValue("id"), limit)
	})
	a.handle("GET /dlq", http.StatusOK, func(ctx context.Context, tenant string, _ *http.Request) (any, error) {
		return svc.DLQ(ctx, tenant)
	})
	return a.mux
}
