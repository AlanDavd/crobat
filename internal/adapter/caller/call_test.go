package caller

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crobat/internal/domain"
)

// local starts a test server and lets the caller reach it (loopback is
// blocked by default, and test servers do not listen on 80/443).
func local(t *testing.T, h http.HandlerFunc) (*Caller, string) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())
	old := allowedPorts
	allowedPorts = map[uint16]bool{80: true, 443: true, uint16(port): true}
	t.Cleanup(func() { allowedPorts = old })
	return New([]netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}), srv.URL
}

func job(url string) domain.Job {
	return domain.Job{
		ID: "job-1", Attempt: 2, TimeoutMS: 1000, ScheduledAt: time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC),
		Request: domain.Request{Method: "POST", URL: url, Headers: map[string]string{"Authorization": "Bearer t0k"}, Body: `{"x":1}`},
	}
}

func TestCallSendsRequestAndHeaders(t *testing.T) {
	var got *http.Request
	var body string
	c, base := local(t, func(w http.ResponseWriter, r *http.Request) {
		got = r
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.Write([]byte("fine"))
	})
	r := c.Call(context.Background(), job(base+"/hook"))
	if !r.OK() || r.Body != "fine" || r.FinishedAt.Before(r.StartedAt) {
		t.Fatalf("result = %+v", r)
	}
	for k, want := range map[string]string{
		"Idempotency-Key": "job-1", "X-Attempt": "2", "X-Scheduled-At": "2026-01-01T09:00:00Z", "Authorization": "Bearer t0k",
	} {
		if got.Header.Get(k) != want {
			t.Errorf("%s = %q, want %q", k, got.Header.Get(k), want)
		}
	}
	if got.Method != "POST" || got.URL.Path != "/hook" || body != `{"x":1}` {
		t.Fatalf("request = %s %s %q", got.Method, got.URL.Path, body)
	}
}

func TestCallReportsNon2xx(t *testing.T) {
	c, base := local(t, func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "nope", 503) })
	if r := c.Call(context.Background(), job(base)); r.OK() || r.HTTPStatus != 503 {
		t.Fatalf("result = %+v", r)
	}
}

func TestCallNeverFollowsRedirects(t *testing.T) {
	var hits atomic.Int32
	c, base := local(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data", http.StatusFound)
	})
	r := c.Call(context.Background(), job(base))
	if r.HTTPStatus != http.StatusFound || r.OK() || hits.Load() != 1 {
		t.Fatalf("a redirect must be a failed attempt, not followed: %+v", r)
	}
}

func TestCallTimesOut(t *testing.T) {
	c, base := local(t, func(_ http.ResponseWriter, r *http.Request) {
		select { // bounded: the server only notices the hang-up after reading the body
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	})
	j := job(base)
	j.TimeoutMS = 100
	if r := c.Call(context.Background(), j); r.Err != "timeout" {
		t.Fatalf("err = %q", r.Err)
	}
}

func TestCallBlocksPrivateDestinationsAtDialTime(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer srv.Close()
	r := New(nil).Call(context.Background(), job(srv.URL+"/?token=s3cret"))
	if r.Err != "destination not allowed" || hits.Load() != 0 {
		t.Fatalf("loopback must be refused before connecting: %+v", r)
	}
	if strings.Contains(r.Err, "s3cret") || strings.Contains(r.Err, "127.0.0.1") {
		t.Fatalf("errors must not leak the URL: %q", r.Err)
	}
}

func TestDescribeDropsTheURL(t *testing.T) {
	c := New(nil)
	r := c.Call(context.Background(), job("http://no-such-host.invalid/?api_key=s3cret"))
	if r.Err == "" || strings.Contains(r.Err, "s3cret") || strings.Contains(r.Err, "no-such-host") {
		t.Fatalf("err = %q", r.Err)
	}
}
