package main

import (
	"testing"
	"time"
)

func TestReadSettings(t *testing.T) {
	cases := []struct {
		name, days, interval string
		want                 settings
		ok                   bool
	}{
		{"loop", "7", "1m", settings{dsn: "postgres://x", olderThan: 7 * 24 * time.Hour, interval: time.Minute}, true},
		{"one pass", "30", "0", settings{dsn: "postgres://x", olderThan: 30 * 24 * time.Hour}, true},
		{"zero days", "0", "1m", settings{}, false},
		{"fractional days", "1.5", "1m", settings{}, false},
		{"bad interval", "7", "soon", settings{}, false},
		{"negative interval", "7", "-1s", settings{}, false},
		{"missing", "", "", settings{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://x")
			t.Setenv("HOLD_EXPIRY_DAYS", tc.days)
			t.Setenv("HOLD_EXPIRY_INTERVAL", tc.interval)
			got, err := readSettings()
			if (err == nil) != tc.ok || got != tc.want {
				t.Errorf("readSettings() = %+v, %v; want %+v, ok %v", got, err, tc.want, tc.ok)
			}
		})
	}
}
