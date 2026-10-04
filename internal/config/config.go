// Package config reads a command's settings from the environment.
//
// Every setting is required. A command that falls back to a default when a
// variable is missing does not fail: it quietly talks to localhost, or to the
// wrong database, and the mistake surfaces somewhere else, later. So a missing
// variable stops the command at startup, and every missing one is named at
// once rather than one per attempt.
//
// Locally the values come from .env, which the Makefile loads. Copy
// .env.example to start.
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
