package retry

import (
	"testing"
	"time"
)

func TestNextDelay(t *testing.T) {
	base := 100 * time.Millisecond
	cap := 30 * time.Second
	tests := []struct {
		name       string
		attempt    int
		retryAfter time.Duration
		remaining  time.Duration
		wantOK     bool
		min        time.Duration
		max        time.Duration
		exact      time.Duration
	}{
		{name: "no time left", attempt: 1, remaining: 0, wantOK: false},
		{name: "half below 1ms", attempt: 1, remaining: time.Millisecond, wantOK: false},
		{name: "retry-after clamped to half remaining", attempt: 3, retryAfter: 5 * time.Second, remaining: 6 * time.Second, wantOK: true, exact: 3 * time.Second},
		{name: "retry-after honored", attempt: 2, retryAfter: time.Second, remaining: 10 * time.Second, wantOK: true, exact: time.Second},
		{name: "exp with jitter stays in range", attempt: 1, remaining: time.Minute, wantOK: true, min: 50 * time.Millisecond, max: 100 * time.Millisecond},
		{name: "exp grows then caps via remaining", attempt: 8, remaining: 400 * time.Millisecond, wantOK: true, exact: 200 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Retry a few times when jitter is involved so a single roll cannot flake the bounds.
			n := 1
			if tt.exact == 0 && tt.wantOK {
				n = 20
			}
			for i := 0; i < n; i++ {
				got, ok := NextDelay(tt.attempt, base, cap, tt.retryAfter, tt.remaining)
				if ok != tt.wantOK {
					t.Fatalf("ok=%v want %v", ok, tt.wantOK)
				}
				if !tt.wantOK {
					return
				}
				if tt.exact > 0 && got != tt.exact {
					t.Fatalf("delay=%s want %s", got, tt.exact)
				}
				if tt.exact == 0 && (got < tt.min || got > tt.max) {
					t.Fatalf("delay=%s outside [%s, %s]", got, tt.min, tt.max)
				}
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		header string
		want   time.Duration
	}{
		{header: "", want: 0},
		{header: "0", want: 0},
		{header: "12", want: 12 * time.Second},
		{header: "-1", want: 0},
		{header: "bogus", want: 0},
		{header: now.Add(5 * time.Second).In(time.FixedZone("GMT", 0)).Format(time.RFC1123), want: 5 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.header, func(t *testing.T) {
			if got := ParseRetryAfter(tt.header, now); got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}

func TestRetryableStatus(t *testing.T) {
	if !RetryableStatus(429) || !RetryableStatus(503) || !RetryableStatus(408) {
		t.Fatal("expected retryable statuses")
	}
	if RetryableStatus(200) || RetryableStatus(400) || RetryableStatus(404) {
		t.Fatal("client statuses should not be retried")
	}
}
