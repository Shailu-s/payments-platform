package webhooks

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

const testSecret = "test-only-webhook-secret-never-use-in-production"

func TestSignMatchesIndependentHMACVector(t *testing.T) {
	got := Sign([]byte(testSecret), "1700000000", []byte(`{"status":"settled"}`))
	want := "v1=523a237624474cf089d84b6f33206b8a54aaa7aab7244432eba31d34eced01d3"
	if got != want {
		t.Fatalf("signature = %s, want %s", got, want)
	}
}

func TestVerifyAuthenticityAndReplayWindow(t *testing.T) {
	now := time.Unix(1700000000, 0)
	secret := []byte(testSecret)
	body := []byte(`{"status":"settled"}`)
	timestamp := strconv.FormatInt(now.Unix(), 10)
	signature := Sign(secret, timestamp, body)
	tests := []struct {
		name      string
		secret    []byte
		timestamp string
		signature string
		body      []byte
		want      bool
	}{
		{"valid", secret, timestamp, signature, body, true},
		{"wrong key", []byte(strings.Repeat("x", 32)), timestamp, signature, body, false},
		{"missing key", nil, timestamp, signature, body, false},
		{"short key", []byte("short"), timestamp, Sign([]byte("short"), timestamp, body), body, false},
		{"missing timestamp", secret, "", signature, body, false},
		{"malformed timestamp", secret, "yesterday", signature, body, false},
		{"timestamp overflow", secret, "9223372036854775808", signature, body, false},
		{"extreme past", secret, "-9223372036854775808", signature, body, false},
		{"extreme future", secret, "9223372036854775807", signature, body, false},
		{"changed timestamp", secret, "1700000001", signature, body, false},
		{"missing signature", secret, timestamp, "", body, false},
		{"wrong version", secret, timestamp, "v2=" + signature[3:], body, false},
		{"malformed hex", secret, timestamp, "v1=" + strings.Repeat("g", 64), body, false},
		{"wrong digest", secret, timestamp, "v1=" + strings.Repeat("0", 64), body, false},
		{"changed body", secret, timestamp, signature, []byte(`{"status":"failed"}`), false},
		{"changed whitespace", secret, timestamp, signature, []byte(`{ "status": "settled" }`), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Verify(tc.secret, tc.timestamp, tc.signature, tc.body, now); got != tc.want {
				t.Errorf("Verify = %t, want %t", got, tc.want)
			}
		})
	}
	for _, offset := range []int64{-301, -300, -299, 299, 300, 301} {
		t.Run(strconv.FormatInt(offset, 10)+" seconds", func(t *testing.T) {
			timestamp := strconv.FormatInt(now.Unix()+offset, 10)
			want := offset >= -300 && offset <= 300
			if got := Verify(secret, timestamp, Sign(secret, timestamp, body), body, now); got != want {
				t.Errorf("Verify at offset %d = %t, want %t", offset, got, want)
			}
		})
	}
}
