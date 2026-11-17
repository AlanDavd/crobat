package mysql

import (
	"bytes"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"

	"crobat/internal/domain"
)

var key = bytes.Repeat([]byte{7}, 32)

// sql.Open is lazy: no database is needed for the pure tests below.
func offline(t *testing.T, k []byte) *Store {
	t.Helper()
	s, err := Open("u:p@tcp(127.0.0.1:1)/x", k)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSecretsRoundTripAndAreNotPlaintext(t *testing.T) {
	s := offline(t, key)
	req := domain.Request{Headers: map[string]string{"Authorization": "Bearer TOPSECRET"}, Body: "BODYSECRET"}
	a, b := s.seal(req), s.seal(req)
	if bytes.Contains(a, []byte("TOPSECRET")) || bytes.Contains(a, []byte("BODYSECRET")) {
		t.Fatal("ciphertext contains plaintext")
	}
	if bytes.Equal(a, b) {
		t.Fatal("nonces must be random: same input gave the same ciphertext")
	}
	var got domain.Request
	if err := s.open(a, &got); err != nil || got.Headers["Authorization"] != "Bearer TOPSECRET" || got.Body != "BODYSECRET" {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	if s.seal(domain.Request{}) != nil {
		t.Fatal("no headers and no body should store NULL")
	}
	if err := offline(t, bytes.Repeat([]byte{8}, 32)).open(a, &got); err == nil {
		t.Fatal("a different key must not decrypt")
	}
	if err := s.open(a[:5], &got); err == nil {
		t.Fatal("truncated ciphertext must fail")
	}
}

func TestOpenRejectsBadKeys(t *testing.T) {
	if _, err := Open("u:p@tcp(127.0.0.1:1)/x", []byte("short")); err == nil {
		t.Fatal("want an error for a bad key")
	}
}

func TestRetryDeadlock(t *testing.T) {
	deadlock := &mysql.MySQLError{Number: 1213}
	calls := 0
	v, err := retryDeadlock(func() (int, error) {
		if calls++; calls < 3 {
			return 0, deadlock
		}
		return 42, nil
	})
	if v != 42 || err != nil || calls != 3 {
		t.Fatalf("v=%d err=%v calls=%d", v, err, calls)
	}

	calls = 0
	if _, err := retryDeadlock(func() (int, error) { calls++; return 0, deadlock }); !errors.Is(err, deadlock) || calls != 5 {
		t.Fatalf("must give up after 5 tries: calls=%d err=%v", calls, err)
	}

	calls = 0
	other := errors.New("syntax error")
	if _, err := retryDeadlock(func() (int, error) { calls++; return 0, other }); err != other || calls != 1 {
		t.Fatalf("other errors must not be retried: calls=%d", calls)
	}
}
