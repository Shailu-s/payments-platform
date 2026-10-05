// Package testdb isolates each test package in its own PostgreSQL schema.
// Shared tables would let parallel packages truncate each other's fixtures.
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

// Connect recreates the named test schema, applies migrations, and returns a scoped pool.
func Connect(ctx context.Context, schema string) (*pgxpool.Pool, error) {
	if schema == "" {
		return nil, fmt.Errorf("testdb: a schema name is required")
	}

	config, err := pgxpool.ParseConfig(DSN())
	if err != nil {
		return nil, fmt.Errorf("testdb: parse %s: %w", DSN(), err)
	}

	// Scope every pooled connection, including connections opened later.
	config.ConnConfig.RuntimeParams["search_path"] = schema

	// Create the schema before opening connections scoped to it.
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
	// An interrupted run must not leave stale tables for this run.
	if _, err := admin.Exec(ctx, fmt.Sprintf(
		`DROP SCHEMA IF EXISTS %s CASCADE; CREATE SCHEMA %s`, quote(schema), quote(schema))); err != nil {
		return fmt.Errorf("testdb: create schema %s: %w", schema, err)
	}
	return nil
}

// Apply ordered migrations directly: these schemas are disposable and need no version table.
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

// TruncateAll discovers and empties the schema's tables, then restores settlement.
// Catalogue discovery prevents new tables leaking state between tests.
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

	// Every transfer needs the settlement account removed by truncation.
	if _, err := pool.Exec(ctx,
		`INSERT INTO accounts (id, currency, type) VALUES ('acc_settlement_usd', 'USD', 'settlement')
		 ON CONFLICT (id) DO NOTHING`); err != nil {
		return fmt.Errorf("testdb: restore settlement account: %w", err)
	}
	return nil
}

func quote(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// ConnectionHint is what a TestMain prints when there is no database.
func ConnectionHint(err error) string {
	return fmt.Sprintf(
		"\nno usable database at %s: %v\n"+
			"run `make up` first, or set TEST_DATABASE_URL\n\n", DSN(), err)
}
