// Package id generates short random identifiers with a stable prefix.
package id

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// New returns prefix plus 24 hex characters (12 random bytes).
func New(prefix string) (string, error) {
	var buf [12]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("random id: %w", err)
	}
	return prefix + hex.EncodeToString(buf[:]), nil
}
