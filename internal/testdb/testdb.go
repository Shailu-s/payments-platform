// Package testdb gives each test package its own isolated schema.
//
// The constraint: `go test ./...` runs packages in parallel, and every package
// here truncates tables before each test. Sharing one schema means one package
// deletes another's rows mid-test, or two truncates deadlock against each
// other — which is exactly what happened: four packages, foreign key
// violations and SQLSTATE 40P01.
//
// `-p 1` hides it by serialising, but a suite that only passes through a
// Makefile flag looks broken to anyone who clones the repository and runs the
// standard command, CI included. It also gets slower with every package added.
//
// So each package connects to the same database with its own search_path,
// migrates into it, and truncates only its own tables. Parallel packages can no
// longer see each other.
package testdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDSN = "postgres://payments:payments@localhost:5433/payments?sslmode=disable"

// DSN returns the database to test against.
func DSN() string {
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		return dsn
	}
	return defaultDSN
}

// Connect returns a pool scoped to its own schema, named after the caller's
// package, with every migration applied inside it.
//
// The returned error is for the caller to report: TestMain wants to print the
// "run make up first" hint rather than have a library decide how to exit.
func Connect(ctx context.Context, schema string) (*pgxpool.Pool, error) {
	if schema == "" {
		return nil, fmt.Errorf("testdb: a schema name is required")
	}

	config, err := pgxpool.ParseConfig(DSN())
	if err != nil {
		return nil, fmt.Errorf("testdb: parse %s: %w", DSN(), err)
	}

	// Set on every connection in the pool, including ones opened later, so a
	// query can never silently land in public.
	config.ConnConfig.RuntimeParams["search_path"] = schema

	// The schema has to exist before any pooled connection sets search_path to
	// it, so this one is created on a connection that does not.
	if err := createSchema(ctx, schema); err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("testdb: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("testdb: ping: %w", err)
	}

	if err := migrate(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func createSchema(ctx context.Context, schema string) error {
	admin, err := pgxpool.New(ctx, DSN())
	if err != nil {
		return fmt.Errorf("testdb: connect: %w", err)
	}
	defer admin.Close()

	if err := admin.Ping(ctx); err != nil {
		return fmt.Errorf("testdb: ping: %w", err)
	}
	// Dropped and recreated, so a schema left behind by an interrupted run
	// cannot carry stale tables into this one.
	if _, err := admin.Exec(ctx, fmt.Sprintf(
		`DROP SCHEMA IF EXISTS %s CASCADE; CREATE SCHEMA %s`, quote(schema), quote(schema))); err != nil {
		return fmt.Errorf("testdb: create schema %s: %w", schema, err)
	}
	return nil
}

// migrate applies every .up.sql file into the schema, in filename order. The
// migrate CLI is not used here because it tracks versions per database rather
// than per schema, and these schemas are disposable.
func migrate(ctx context.Context, schema string) error {
	pool, err := connectTo(ctx, schema)
	if err != nil {
		return err
	}
	defer pool.Close()

	dir := migrationsDir()
	entries, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		return fmt.Errorf("testdb: find migrations: %w", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("testdb: no migrations found in %s", dir)
	}

	for _, path := range entries {
		statements, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("testdb: read %s: %w", path, err)
		}
		if _, err := pool.Exec(ctx, string(statements)); err != nil {
			return fmt.Errorf("testdb: apply %s: %w", filepath.Base(path), err)
		}
	}
	return nil
}

func connectTo(ctx context.Context, schema string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(DSN())
	if err != nil {
		return nil, fmt.Errorf("testdb: parse dsn: %w", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	return pgxpool.NewWithConfig(ctx, config)
}

// migrationsDir locates db/migrations relative to this source file, so tests
// work whatever directory they are run from.
func migrationsDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "db", "migrations")
}

// TruncateAll empties every table in the schema. The list comes from the
// catalogue rather than being written by hand, because a hand-written list goes
// stale the moment a migration adds a table and the failure surfaces as a
// foreign key error in an unrelated package.
func TruncateAll(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	const q = `
		SELECT string_agg(format('%I.%I', schemaname, tablename), ', ')
		FROM pg_tables WHERE schemaname = $1`

	var tables *string
	if err := pool.QueryRow(ctx, q, schema).Scan(&tables); err != nil {
		return fmt.Errorf("testdb: list tables: %w", err)
	}
	if tables == nil {
		return nil
	}
	if _, err := pool.Exec(ctx, "TRUNCATE "+*tables+" CASCADE"); err != nil {
		return fmt.Errorf("testdb: truncate: %w", err)
	}

	// Migration 000003 creates the settlement account and truncating removes
	// it, while every transfer credits it. Restored here rather than in each
	// test, so a forgotten setup cannot make a test pass for the wrong reason.
	if _, err := pool.Exec(ctx,
		`INSERT INTO accounts (id, currency, type) VALUES ('acc_settlement_usd', 'USD', 'settlement')
		 ON CONFLICT (id) DO NOTHING`); err != nil {
		return fmt.Errorf("testdb: restore settlement account: %w", err)
	}
	return nil
}

// quote renders an identifier safely. Schema names here are compile-time
// constants, but building SQL by concatenation without one is a habit worth not
// forming.
func quote(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// ConnectionHint is what a TestMain prints when there is no database.
func ConnectionHint(err error) string {
	return fmt.Sprintf(
		"\nno usable database at %s: %v\n"+
			"run `make up` first, or set TEST_DATABASE_URL\n\n", DSN(), err)
}
