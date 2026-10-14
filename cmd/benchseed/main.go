// Command benchseed fills a disposable benchmark database with funded accounts
// and API keys, then writes the plaintext keys to a local fixture file.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/config"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const settlementAccountID = "acc_settlement_usd"

type fixture struct {
	Keys         []string `json:"keys"`
	Sources      []string `json:"sources"`
	Destinations []string `json:"destinations"`
	HotSource    string   `json:"hot_source"`
}

func main() {
	accounts := flag.Int("accounts", 1000, "funded source accounts (and as many destinations)")
	keys := flag.Int("keys", 3000, "API keys; the per-key limit is 100 requests per minute")
	funding := flag.Int64("funding", 10_000_000_000, "cents credited to each source account")
	out := flag.String("out", "bench/out/fixture.json", "fixture file holding plaintext keys")
	flag.Parse()

	var env config.Env
	dsn := env.Require("BENCH_DATABASE_URL")
	if err := env.Err(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	if err := run(context.Background(), dsn, *accounts, *keys, *funding, *out); err != nil {
		slog.Error("seed failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dsn string, accounts, keys int, funding int64, out string) error {
	// Refuse anything but a benchmark database: seeding writes money.
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(cfg.ConnConfig.Database, "payments_bench") {
		return fmt.Errorf("database %q is not a payments_bench* database", cfg.ConnConfig.Database)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	f := fixture{HotSource: "acc_bench_hot"}
	rows := [][]any{{f.HotSource, "USD", "asset"}}
	for i := range accounts {
		src, dst := fmt.Sprintf("acc_bench_src_%05d", i), fmt.Sprintf("acc_bench_dst_%05d", i)
		f.Sources = append(f.Sources, src)
		f.Destinations = append(f.Destinations, dst)
		rows = append(rows, []any{src, "USD", "asset"}, []any{dst, "USD", "asset"})
	}
	if _, err := pool.CopyFrom(ctx, pgx.Identifier{"accounts"},
		[]string{"id", "currency", "type"}, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("copy accounts: %w", err)
	}

	// One balanced funding transaction through the production ledger writer.
	funded := append([]string{f.HotSource}, f.Sources...)
	entries := []ledger.Entry{{AccountID: settlementAccountID, Direction: ledger.DirectionDebit,
		Amount: funding * int64(len(funded))}}
	for _, id := range funded {
		entries = append(entries, ledger.Entry{AccountID: id, Direction: ledger.DirectionCredit, Amount: funding})
	}
	if _, err := ledger.Record(ctx, pool, "benchmark funding", entries); err != nil {
		return err
	}

	keyRows := make([][]any, 0, keys)
	for i := range keys {
		plaintext, k, err := auth.Generate(fmt.Sprintf("bench-%d", i))
		if err != nil {
			return err
		}
		f.Keys = append(f.Keys, plaintext)
		keyRows = append(keyRows, []any{k.ID, k.Hash, k.Prefix, k.Name})
	}
	if _, err := pool.CopyFrom(ctx, pgx.Identifier{"api_keys"},
		[]string{"id", "key_hash", "prefix", "name"}, pgx.CopyFromRows(keyRows)); err != nil {
		return fmt.Errorf("copy api keys: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(out), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if err := os.WriteFile(out, data, 0o600); err != nil {
		return err
	}
	slog.Info("seeded", "accounts", accounts, "keys", keys, "fixture", out)
	return nil
}
