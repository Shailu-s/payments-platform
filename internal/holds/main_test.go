package holds

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/cards"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testSchema = "test_holds"

const settlementAccountID = "acc_settlement_usd"

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	pool, err := testdb.Connect(ctx, testSchema)
	if err != nil {
		fmt.Fprint(os.Stderr, testdb.ConnectionHint(err))
		if os.Getenv("LEDGER_TESTS") == "skip" {
			os.Exit(0)
		}
		os.Exit(1)
	}
	testPool = pool

	code := m.Run()
	pool.Close()
	os.Exit(code)
}

func resetDB(tb testing.TB) {
	tb.Helper()
	truncate := func() {
		if err := testdb.TruncateAll(context.Background(), testPool, testSchema); err != nil {
			tb.Fatalf("reset: %v", err)
		}
	}
	truncate()
	tb.Cleanup(truncate)
}

// newCard issues a card on a fresh funding account holding balance.
func newCard(tb testing.TB, balance int64) cards.Card {
	tb.Helper()
	ctx := context.Background()
	_, key, err := auth.Generate("holds test")
	if err != nil {
		tb.Fatal(err)
	}
	if err := auth.Insert(ctx, testPool, key); err != nil {
		tb.Fatal(err)
	}
	funding := newID("acc")
	if _, err := testPool.Exec(ctx, `INSERT INTO accounts (id, currency, type) VALUES ($1, 'USD', 'asset')`, funding); err != nil {
		tb.Fatal(err)
	}
	if balance > 0 {
		if _, err := ledger.Record(ctx, testPool, "funding", []ledger.Entry{
			{AccountID: settlementAccountID, Direction: ledger.DirectionDebit, Amount: balance},
			{AccountID: funding, Direction: ledger.DirectionCredit, Amount: balance},
		}); err != nil {
			tb.Fatal(err)
		}
	}
	card, err := cards.Issue(ctx, testPool, cards.Card{
		ID: newID("card"), AccountID: funding, PerTransactionLimit: 1_000_000, DailyLimit: 1_000_000,
		APIKeyID: key.ID, IdempotencyKey: newID("idem"), RequestFingerprint: "test",
	})
	if err != nil {
		tb.Fatal(err)
	}
	return card
}

// placeHold stores an approved authorization holding amount, as the
// authorization path does: row, daily spend, then the ledger move.
func placeHold(tb testing.TB, card cards.Card, authID string, amount int64) {
	tb.Helper()
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer tx.Rollback(ctx)
	txnID := ledger.NewTxnID()
	if _, err := tx.Exec(ctx, `
		INSERT INTO card_authorizations (auth_id, card_id, account_id, held_account_id, amount,
			currency, merchant_id, merchant_name, mcc, kind, decision, approved_amount,
			ledger_txn_id, request_fingerprint, held_amount)
		VALUES ($1, $2, $3, $4, $5, 'USD', 'mer_test', 'Test', '5814', 'initial', 'approved', $5, $6, 'test', $5)`,
		authID, card.ID, card.AccountID, card.HeldAccountID, amount, txnID); err != nil {
		tb.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO card_daily_spend (card_id, day, amount) VALUES ($1, (now() AT TIME ZONE 'UTC')::date, $2)
		ON CONFLICT (card_id, day) DO UPDATE SET amount = card_daily_spend.amount + EXCLUDED.amount`,
		card.ID, amount); err != nil {
		tb.Fatal(err)
	}
	if err := ledger.RecordWithID(ctx, tx, txnID, "authorization "+authID, []ledger.Entry{
		{AccountID: card.AccountID, Direction: ledger.DirectionDebit, Amount: amount},
		{AccountID: card.HeldAccountID, Direction: ledger.DirectionCredit, Amount: amount},
	}); err != nil {
		tb.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		tb.Fatal(err)
	}
}

func storedBalance(tb testing.TB, accountID string) int64 {
	tb.Helper()
	balance, _, err := ledger.StoredBalance(context.Background(), testPool, accountID)
	if err != nil {
		tb.Fatal(err)
	}
	return balance
}

func assertNoDrift(tb testing.TB) {
	tb.Helper()
	drifts, err := ledger.VerifyBalances(context.Background(), testPool)
	if err != nil {
		tb.Fatal(err)
	}
	if len(drifts) != 0 {
		tb.Errorf("stored balances drifted from the ledger: %+v", drifts)
	}
}

func countWhere(tb testing.TB, q string, args ...any) int {
	tb.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		tb.Fatalf("%s: %v", q, err)
	}
	return n
}

// release runs Release in its own transaction.
func release(ctx context.Context, authID string, amount int64, reason, messageID string) (int64, error) {
	tx, err := testPool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	n, err := Release(ctx, tx, authID, amount, reason, messageID)
	if err != nil {
		return 0, err
	}
	return n, tx.Commit(ctx)
}
