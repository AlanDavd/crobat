// Command crobat runs any of the six services (`crobat serve <name>`) and is
// also the tenant CLI that talks to the task and job APIs.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"crobat/internal/adapter/caller"
	"crobat/internal/adapter/cassandra"
	"crobat/internal/adapter/httpapi"
	"crobat/internal/adapter/mysql"
	"crobat/internal/app"
)

const usage = `crobat: cron as a service.

Services (configured by env, see ARCHITECTURE.md):
  crobat serve task-api|job-api|scheduler|executor|reconciler|archiver|echo

CLI (env CROBAT_TOKEN, CROBAT_TASK_API, CROBAT_JOB_API):
  crobat token <tenant>                 mint a dev JWT (needs JWT_SECRET)
  crobat task create [flags]            -h for flags
  crobat task update <id> [flags]       full replace, same flags as create
  crobat task list
  crobat task get|pause|resume|delete <id>
  crobat job get <id>
  crobat job history <task-id> [-limit n]
  crobat job dlq
  crobat job retry <id>
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(arg(2))
	case "token":
		err = token(arg(2))
	case "task":
		err = taskCmd(arg(2), os.Args[min(3, len(os.Args)):])
	case "job":
		err = jobCmd(arg(2), os.Args[min(3, len(os.Args)):])
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func arg(i int) string {
	if i < len(os.Args) {
		return os.Args[i]
	}
	return ""
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDur(key, def string) time.Duration {
	d, err := time.ParseDuration(env(key, def))
	if err != nil {
		panic(fmt.Sprintf("%s: %v", key, err))
	}
	return d
}

func envInt(key string, def int) int {
	n, err := strconv.Atoi(env(key, strconv.Itoa(def)))
	if err != nil {
		panic(fmt.Sprintf("%s: %v", key, err))
	}
	return n
}

func mustEnv(key string) (string, error) {
	if v := os.Getenv(key); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("%s is required", key)
}

// ------------------------------------------------------------------- services

func serve(name string) error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", name)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if name == "echo" {
		return listen(ctx, env("HTTP_ADDR", ":80"), echo(log), log)
	}

	keyB64, err := mustEnv("SECRETS_KEY")
	if err != nil {
		return err
	}
	key, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil || len(key) != 32 {
		return errors.New("SECRETS_KEY must be 32 bytes, base64-encoded (openssl rand -base64 32)")
	}
	db, err := mysql.Open(env("MYSQL_DSN", "crobat:crobat@tcp(localhost:3306)/crobat"), key)
	if err != nil {
		return err
	}
	if err := db.Ping(ctx); err != nil {
		return fmt.Errorf("mysql: %w", err)
	}
	history := func() (*cassandra.History, error) {
		return cassandra.Open(strings.Split(env("CASSANDRA_HOSTS", "localhost"), ","),
			env("CASSANDRA_KEYSPACE", "crobat"), envInt("CASSANDRA_RF", 1))
	}
	jwtSecret := func() ([]byte, error) { s, err := mustEnv("JWT_SECRET"); return []byte(s), err }

	log.Info("starting")
	switch name {
	case "task-api":
		secret, err := jwtSecret()
		if err != nil {
			return err
		}
		return listen(ctx, env("HTTP_ADDR", ":8080"), httpapi.TaskAPI(&app.TaskService{Tasks: db}, secret, log), log)
	case "job-api":
		secret, err := jwtSecret()
		if err != nil {
			return err
		}
		h, err := history()
		if err != nil {
			return fmt.Errorf("cassandra: %w", err)
		}
		defer h.Close()
		return listen(ctx, env("HTTP_ADDR", ":8081"), httpapi.JobAPI(&app.JobService{Jobs: db, History: h}, secret, log), log)
	case "scheduler":
		(&app.Scheduler{Tasks: db, Interval: envDur("SCHEDULER_INTERVAL", "5s"),
			Lookahead: envDur("SCHEDULER_LOOKAHEAD", "30s"), Batch: envInt("SCHEDULER_BATCH", 500), Log: log}).Run(ctx)
	case "executor":
		var allow []netip.Prefix
		for _, s := range strings.Split(os.Getenv("EXECUTOR_ALLOW_CIDRS"), ",") {
			if s = strings.TrimSpace(s); s != "" {
				p, err := netip.ParsePrefix(s)
				if err != nil {
					return fmt.Errorf("EXECUTOR_ALLOW_CIDRS: %w", err)
				}
				allow = append(allow, p)
			}
		}
		if len(allow) > 0 {
			log.Warn("SSRF allowlist active: dev only", "cidrs", allow)
		}
		host, _ := os.Hostname()
		(&app.Executor{Jobs: db, Caller: caller.New(allow), WorkerID: env("WORKER_ID", host),
			Workers: envInt("EXECUTOR_WORKERS", 64), Poll: envDur("EXECUTOR_POLL", "500ms"), Log: log}).Run(ctx)
	case "reconciler":
		(&app.Reconciler{Jobs: db, Interval: envDur("RECONCILER_INTERVAL", "5s"), Batch: 1000, Log: log}).Run(ctx)
	case "archiver":
		h, err := history()
		if err != nil {
			return fmt.Errorf("cassandra: %w", err)
		}
		defer h.Close()
		(&app.Archiver{Jobs: db, History: h, Interval: envDur("ARCHIVER_INTERVAL", "10s"), Batch: 500,
			KeepSucceeded: envDur("ARCHIVE_SUCCEEDED_AFTER", "1h"), KeepDead: envDur("ARCHIVE_DEAD_AFTER", "336h"), Log: log}).Run(ctx)
	default:
		return fmt.Errorf("unknown service %q", name)
	}
	log.Info("stopped")
	return nil
}

func listen(ctx context.Context, addr string, h http.Handler, log *slog.Logger) error {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("listening", "addr", addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// echo is a demo target: /fail always 500s, /flaky 503s on the first
// attempt, /slow?ms=N sleeps, anything else 200s.
func echo(log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt, _ := strconv.Atoi(r.Header.Get("X-Attempt"))
		sched, _ := time.Parse(time.RFC3339, r.Header.Get("X-Scheduled-At"))
		log.Info("received", "method", r.Method, "path", r.URL.Path, "idempotency_key", r.Header.Get("Idempotency-Key"),
			"attempt", attempt, "lag_ms", time.Since(sched).Milliseconds())
		switch r.URL.Path {
		case "/fail":
			http.Error(w, "boom", http.StatusInternalServerError)
		case "/flaky":
			if attempt < 2 {
				http.Error(w, "try again", http.StatusServiceUnavailable)
				return
			}
			fmt.Fprintln(w, "ok on retry")
		case "/slow":
			ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
			time.Sleep(time.Duration(ms) * time.Millisecond)
			fmt.Fprintln(w, "ok, slowly")
		default:
			fmt.Fprintln(w, "ok")
		}
	})
}

// ------------------------------------------------------------------------ CLI

func token(tenant string) error {
	if tenant == "" {
		return errors.New("usage: crobat token <tenant>")
	}
	secret, err := mustEnv("JWT_SECRET")
	if err != nil {
		return err
	}
	fmt.Println(httpapi.SignToken([]byte(secret), tenant, 30*24*time.Hour))
	return nil
}

// call sends an authenticated request and pretty-prints the JSON reply.
func call(method, base, path string, body any, headers map[string]string) error {
	tok, err := mustEnv("CROBAT_TOKEN")
	if err != nil {
		return fmt.Errorf("%w (export CROBAT_TOKEN=$(crobat token <tenant>))", err)
	}
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, strings.TrimSuffix(base, "/")+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	var pretty bytes.Buffer
	if json.Indent(&pretty, out, "", "  ") == nil {
		out = pretty.Bytes()
	}
	fmt.Println(strings.TrimSpace(string(out)))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s", resp.Status)
	}
	return nil
}

func taskAPI() string { return env("CROBAT_TASK_API", "http://localhost:8080") }
func jobAPI() string  { return env("CROBAT_JOB_API", "http://localhost:8081") }

type headerFlags map[string]string

func (h headerFlags) String() string { return "" }
func (h headerFlags) Set(s string) error {
	k, v, ok := strings.Cut(s, ":")
	if !ok {
		return errors.New(`want "Name: value"`)
	}
	h[strings.TrimSpace(k)] = strings.TrimSpace(v)
	return nil
}

// taskBody parses create/update flags into the POST/PUT /tasks body.
func taskBody(args []string) (map[string]any, string, error) {
	fs := flag.NewFlagSet("task", flag.ContinueOnError)
	name := fs.String("name", "", "task name (required)")
	url := fs.String("url", "", "endpoint to call (required)")
	method := fs.String("method", "POST", "HTTP method")
	body := fs.String("body", "", "request body (encrypted at rest)")
	headers := headerFlags{}
	fs.Var(headers, "header", `request header "Name: value", repeatable (encrypted at rest)`)
	cronExpr := fs.String("cron", "", `cron expression, e.g. "0 9 * * *"`)
	at := fs.String("at", "", "run once at this RFC3339 time")
	tz := fs.String("tz", "UTC", "IANA timezone for -cron")
	prio := fs.String("priority", "normal", "critical|high|normal|low")
	timeout := fs.Int("timeout-ms", 10000, "per-attempt timeout")
	attempts := fs.Int("max-attempts", 3, "attempts before the DLQ")
	backoff := fs.Int("backoff-ms", 1000, "base retry backoff (doubles per attempt)")
	idem := fs.String("idempotency-key", "", "makes create safe to repeat")
	if err := fs.Parse(args); err != nil {
		return nil, "", err
	}
	b := map[string]any{
		"name": *name, "cron": *cronExpr, "timezone": *tz, "priority": *prio, "timeout_ms": *timeout,
		"request": map[string]any{"method": *method, "url": *url, "headers": headers, "body": *body},
		"retry":   map[string]any{"max_attempts": *attempts, "backoff_ms": *backoff},
	}
	if *at != "" {
		b["at"] = *at
	}
	return b, *idem, nil
}

func taskCmd(sub string, args []string) error {
	id := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		id, args = args[0], args[1:]
	}
	needID := func() error {
		if id == "" {
			return fmt.Errorf("usage: crobat task %s <id>", sub)
		}
		return nil
	}
	switch sub {
	case "create":
		b, idem, err := taskBody(args)
		if err != nil {
			return err
		}
		h := map[string]string{}
		if idem != "" {
			h["Idempotency-Key"] = idem
		}
		return call("POST", taskAPI(), "/tasks", b, h)
	case "update":
		if err := needID(); err != nil {
			return err
		}
		b, _, err := taskBody(args)
		if err != nil {
			return err
		}
		return call("PUT", taskAPI(), "/tasks/"+id, b, nil)
	case "list":
		return call("GET", taskAPI(), "/tasks", nil, nil)
	case "get":
		if err := needID(); err != nil {
			return err
		}
		return call("GET", taskAPI(), "/tasks/"+id, nil, nil)
	case "pause", "resume":
		if err := needID(); err != nil {
			return err
		}
		return call("POST", taskAPI(), "/tasks/"+id+"/"+sub, nil, nil)
	case "delete":
		if err := needID(); err != nil {
			return err
		}
		return call("DELETE", taskAPI(), "/tasks/"+id, nil, nil)
	}
	fmt.Fprint(os.Stderr, usage)
	return errors.New("unknown task command")
}

func jobCmd(sub string, args []string) error {
	fs := flag.NewFlagSet("job", flag.ContinueOnError)
	limit := fs.Int("limit", 50, "max jobs to return")
	id := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		id, args = args[0], args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if id == "" && sub != "dlq" {
		return fmt.Errorf("usage: crobat job %s <id>", sub)
	}
	switch sub {
	case "get":
		return call("GET", jobAPI(), "/jobs/"+id, nil, nil)
	case "retry":
		return call("POST", jobAPI(), "/jobs/"+id+"/retry", nil, nil)
	case "history":
		return call("GET", jobAPI(), fmt.Sprintf("/tasks/%s/jobs?limit=%d", id, *limit), nil, nil)
	case "dlq":
		return call("GET", jobAPI(), "/dlq", nil, nil)
	}
	fmt.Fprint(os.Stderr, usage)
	return errors.New("unknown job command")
}
