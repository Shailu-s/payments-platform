// Package config reads required environment settings, reporting all missing
// values together rather than silently selecting the wrong endpoint.
package config

import (
	"fmt"
	"os"
	"strings"
)

// Env collects required variables. Read each with Require, then check Err once.
type Env struct {
	missing []string
}

// Require returns the variable's value, and records it as missing if it is
// unset or empty.
func (e *Env) Require(name string) string {
	v := os.Getenv(name)
	if v == "" {
		e.missing = append(e.missing, name)
	}
	return v
}

// Err reports every variable that Require found missing.
func (e *Env) Err() error {
	if len(e.missing) == 0 {
		return nil
	}
	return fmt.Errorf("missing environment variables: %s — copy .env.example to .env and run through make",
		strings.Join(e.missing, ", "))
}
