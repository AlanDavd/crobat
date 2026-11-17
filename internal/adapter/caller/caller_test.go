package caller

import (
	"net/netip"
	"testing"
)

func TestAllowed(t *testing.T) {
	for addr, want := range map[string]bool{
		"93.184.216.34:443":      true,
		"93.184.216.34:80":       true,
		"93.184.216.34:8080":     false, // port
		"127.0.0.1:80":           false,
		"10.1.2.3:443":           false,
		"172.16.0.1:443":         false,
		"192.168.1.1:80":         false,
		"169.254.169.254:80":     false, // cloud metadata
		"100.64.0.1:80":          false,
		"0.0.0.0:80":             false,
		"[::1]:443":              false,
		"[::ffff:127.0.0.1]:443": false, // v4-mapped loopback
		"[fd00::1]:443":          false,
		"[2606:4700::1111]:443":  true,
		"[64:ff9b::a00:1]:443":   false,
	} {
		if got := Allowed(addr, nil); got != want {
			t.Errorf("Allowed(%s) = %v, want %v", addr, got, want)
		}
	}
	allow := []netip.Prefix{netip.MustParsePrefix("172.28.0.100/32")}
	if !Allowed("172.28.0.100:80", allow) || Allowed("172.28.0.101:80", allow) || Allowed("172.28.0.100:3306", allow) {
		t.Error("dev allowlist must be exact and keep the port rule")
	}
}
