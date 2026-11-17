// Package caller implements app.Caller with an SSRF guard in the dialer:
// the check runs after DNS resolution, on the exact IP being dialed, so DNS
// rebinding cannot slip past it.
package caller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"crobat/internal/domain"
)

var errBlocked = errors.New("destination not allowed")

// blocked are non-public ranges that Go's IsPrivate/IsLoopback/... miss.
var blocked = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // CGNAT
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("64:ff9b::/96"), // NAT64 can reach IPv4 internals
}

// Allowed reports whether address ("ip:port") may be dialed. allow lists
// extra prefixes (local development only).
func Allowed(address string, allow []netip.Prefix) bool {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return false
	}
	ip := ap.Addr().Unmap()
	if ap.Port() != 80 && ap.Port() != 443 {
		return false
	}
	for _, p := range allow {
		if p.Contains(ip) {
			return true
		}
	}
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false // covers 10/8, 172.16/12, 192.168/16, 127/8, 169.254/16 (metadata), fc00::/7
	}
	for _, p := range blocked {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

type Caller struct{ client *http.Client }

func New(allow []netip.Prefix) *Caller {
	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			if !Allowed(address, allow) {
				return errBlocked
			}
			return nil
		},
	}
	return &Caller{client: &http.Client{
		Transport: &http.Transport{
			Proxy:               nil, // never via a proxy
			DialContext:         dialer.DialContext,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (c *Caller) Call(ctx context.Context, j domain.Job) (r domain.Result) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(j.TimeoutMS)*time.Millisecond)
	defer cancel()
	r.StartedAt = time.Now().UTC()
	defer func() { r.FinishedAt = time.Now().UTC() }()
	req, err := http.NewRequestWithContext(ctx, j.Request.Method, j.Request.URL, strings.NewReader(j.Request.Body))
	if err != nil {
		r.Err = "bad request"
		return r
	}
	for k, v := range j.Request.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Idempotency-Key", j.ID)
	req.Header.Set("X-Scheduled-At", j.ScheduledAt.UTC().Format(time.RFC3339))
	req.Header.Set("X-Attempt", strconv.Itoa(j.Attempt))
	req.Header.Set("User-Agent", "crobat-executor")
	resp, err := c.client.Do(req)
	if err != nil {
		r.Err = describe(err)
		return r
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	r.HTTPStatus, r.Body = resp.StatusCode, string(body)
	return r
}

// describe drops the URL (it may carry secrets) and keeps the cause generic.
func describe(err error) string {
	switch {
	case errors.Is(err, errBlocked):
		return errBlocked.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err
	}
	var oe *net.OpError
	if errors.As(err, &oe) {
		return fmt.Sprintf("network error: %s", oe.Op)
	}
	return "request failed"
}
