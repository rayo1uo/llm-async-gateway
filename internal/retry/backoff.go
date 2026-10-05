// Package retry computes dispatch backoff that honors Retry-After and the remaining deadline.
package retry

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// NextDelay returns how long to park a retryable failure.
// Retry-After wins over exponential backoff. The delay is clamped to half of
// the time still left before the deadline so a retry can still finish.
// ok is false when the request should be terminated instead of retried.
func NextDelay(attempt int, base, cap, retryAfter, remaining time.Duration) (delay time.Duration, ok bool) {
	if remaining <= 0 || base <= 0 {
		return 0, false
	}
	half := remaining / 2
	if half < time.Millisecond {
		return 0, false
	}

	if retryAfter > 0 {
		delay = retryAfter
	} else {
		shift := attempt - 1
		if shift < 0 {
			shift = 0
		}
		if shift > 16 {
			shift = 16
		}
		delay = base << shift
		if delay <= 0 || delay > cap {
			delay = cap
		}
		// Equal jitter: half deterministic, half random, so synchronized workers
		// do not retry on the same instant.
		span := delay / 2
		if span > 0 {
			delay = span + time.Duration(rand.Int64N(int64(span)+1))
		}
	}
	if delay > half {
		delay = half
	}
	if delay <= 0 {
		return 0, false
	}
	return delay, true
}

// ParseRetryAfter parses an HTTP Retry-After header (delta-seconds or HTTP-date).
// An empty or unparseable value returns 0.
func ParseRetryAfter(header string, now time.Time) time.Duration {
	if header == "" {
		return 0
	}
	if secs, err := strconv.Atoi(header); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil {
		d := when.Sub(now)
		if d < 0 {
			return 0
		}
		return d
	}
	return 0
}

// RetryableStatus reports whether an upstream HTTP status should be retried.
func RetryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || code == http.StatusRequestTimeout || code >= 500
}
