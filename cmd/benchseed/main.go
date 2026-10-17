// Command benchseed fills a disposable benchmark database with funded accounts
// and API keys, then writes the plaintext keys to a local fixture file. With
// -verify it instead prints how many stored balances disagree with the ledger.
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
	history := flag.Int("history", 0, "one-cent entries the hot account already has before the run")
	verify := flag.Bool("verify", false, "print the number of drifted stored balances instead of seeding")
	flag.Parse()

	var env config.Env
	dsn := env.Require("BENCH_DATABASE_URL")
	if err := env.Err(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	ctx := context.Background()
	pool, err := connect(ctx, dsn)
	if err != nil {
		slog.Error("connect failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if *verify {
		drifts, err := ledger.VerifyBalances(ctx, pool)
		if err != nil {
			slog.Error("verify failed", "error", err)
			os.Exit(1)
		}
		for _, d := range drifts {
			slog.Error("balance drift", "account", d.AccountID, "stored", d.Stored,
				"derived", d.Derived, "missing_row", d.Missing)
		}
		fmt.Println(len(drifts))
		return
	}
	if err := run(ctx, pool, *accounts, *keys, *funding, *history, *out); err != nil {
		slog.Error("seed failed", "error", err)
		os.Exit(1)
	}
}

// connect refuses anything but a benchmark database: seeding writes money.
func connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(cfg.ConnConfig.Database, "payments_bench") {
		return nil, fmt.Errorf("database %q is not a payments_bench* database", cfg.ConnConfig.Database)
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

func run(ctx context.Context, pool *pgxpool.Pool, accounts, keys int, funding int64, history int, out string) error {

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
	if history > 0 {
		if err := seedHistory(ctx, pool, f.HotSource, history); err != nil {
			return err
		}
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

// seedHistory gives an account n one-cent credits in one balanced transaction,
// so a run can show whether spending cost grows with history. Bulk SQL is far
// faster than n calls to ledger.Record, but it bypasses it, so it applies the
// stored balance itself.
func seedHistory(ctx context.Context, pool *pgxpool.Pool, accountID string, n int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("seed history: %w", err)
	}
	defer tx.Rollback(ctx)

	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO ledger_transactions (id, reference) VALUES ('ltx_bench_history', 'benchmark history')`, nil},
		{`INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount)
		  SELECT 'le_bench_history_' || g, 'ltx_bench_history', $1, 'credit', 1
		  FROM generate_series(1, $2::int) g`, []any{accountID, n}},
		{`INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount)
		  VALUES ('le_bench_history_debit', 'ltx_bench_history', $1, 'debit', $2)`, []any{settlementAccountID, n}},
		{`UPDATE account_balances SET balance = balance + $2 WHERE account_id = $1`, []any{accountID, n}},
	}
	for _, st := range statements {
		if _, err := tx.Exec(ctx, st.sql, st.args...); err != nil {
			return fmt.Errorf("seed history: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("seed history: commit: %w", err)
	}
	// Settle the bulk insert before the run: plan against the seeded size, and
	// leave no autovacuum or checkpoint of a million fresh rows to land on the
	// first steps.
	for _, maintenance := range []string{`VACUUM (ANALYZE) ledger_entries`, `CHECKPOINT`} {
		if _, err := pool.Exec(ctx, maintenance); err != nil {
			return fmt.Errorf("seed history: %s: %w", maintenance, err)
		}
	}
	slog.Info("seeded history", "account", accountID, "entries", n)
	return nil
}
