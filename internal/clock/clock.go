// Package clock supplies the time source used by schedulers so tests can pin "now".
package clock

import "time"

// Clock returns the current time.
type Clock interface {
	Now() time.Time
}

// Real is the wall clock.
type Real struct{}

// Now returns time.Now().
func (Real) Now() time.Time { return time.Now() }
