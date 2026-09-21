// Command apikey mints an API key and prints it once.
//
// There is no POST /api_keys endpoint in V1: an unauthenticated endpoint that
// mints credentials is worse than no endpoint at all, and an authenticated one
// needs a first key that has to come from somewhere anyway.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDSN = "postgres://payments:payments@localhost:5433/payments?sslmode=disable"

func main() {
	name := flag.String("name", "", "what this key is for, shown in logs and the dashboard")
	revoke := flag.String("revoke", "", "id of a key to revoke instead of minting one")
	list := flag.Bool("list", false, "list keys: id, prefix, name, status. Never the secret")
	flag.Parse()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = defaultDSN
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		fail("no database at %s: %v\nrun `make up && make migrate-up` first", dsn, err)
	}
	defer pool.Close()

	switch {
	case *list:
		listKeys(ctx, pool)
	case *revoke != "":
		if err := auth.Revoke(ctx, pool, *revoke); err != nil {
			fail("%v", err)
		}
		fmt.Printf("revoked %s\n", *revoke)
	default:
		if *name == "" {
			fail("usage: apikey -name \"what this key is for\"")
		}
		mint(ctx, pool, *name)
	}
}

func mint(ctx context.Context, pool *pgxpool.Pool, name string) {
	plaintext, key, err := auth.Generate(name)
	if err != nil {
		fail("%v", err)
	}
	if err := auth.Insert(ctx, pool, key); err != nil {
		fail("%v", err)
	}

	// Printed once. Only the hash was stored, so this cannot be recovered.
	fmt.Printf("\n  id      %s\n", key.ID)
	fmt.Printf("  name    %s\n", key.Name)
	fmt.Printf("  key     %s\n", plaintext)
	fmt.Printf("\n  This is the only time the key is shown. Only its hash was stored.\n\n")
}

func listKeys(ctx context.Context, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx, `
		SELECT id, prefix, name, created_at, revoked_at
		FROM api_keys ORDER BY created_at DESC`)
	if err != nil {
		fail("list keys: %v", err)
	}
	defer rows.Close()

	fmt.Printf("\n%-35s  %-10s  %-24s  %s\n", "ID", "PREFIX", "NAME", "STATUS")
	for rows.Next() {
		var key auth.Key
		if err := rows.Scan(&key.ID, &key.Prefix, &key.Name, &key.CreatedAt, &key.RevokedAt); err != nil {
			fail("scan key: %v", err)
		}
		status := "live"
		if key.RevokedAt != nil {
			status = "revoked " + key.RevokedAt.Format("2006-01-02")
		}
		fmt.Printf("%-35s  %-10s  %-24s  %s\n", key.ID, key.Prefix, key.Name, status)
	}
	if err := rows.Err(); err != nil {
		fail("list keys: %v", err)
	}
	fmt.Println()
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "\n"+format+"\n\n", args...)
	os.Exit(1)
}
