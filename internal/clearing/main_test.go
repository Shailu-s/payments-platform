package clearing

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/cards"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testSchema = "test_clearing"

const settlementAccountID = "acc_settlement_usd"

var testPool *pgxpool.Pool

var (
	testDate   = time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	testConfig = Config{OverTolerancePct: 20}
)

func TestMain(m *testing.M) {
	pool, err := testdb.Connect(context.Background(), testSchema)
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
	_, key, err := auth.Generate("clearing test")
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

// purchase is a clearing line for testDate.
func purchase(clearingID, authID, cardID string, amount int64) Line {
	return Line{
		ClearingID: clearingID, AuthID: authID, CardID: cardID, Kind: KindPurchase, Amount: amount,
		Currency: Currency, MerchantID: "mer_test", ClearedAt: testDate.Add(10 * time.Hour),
	}
}

// fileOf writes lines as the network does.
func fileOf(lines ...Line) []byte {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(header)
	for _, l := range lines {
		_ = w.Write([]string{l.ClearingID, l.AuthID, l.CardID, l.Kind, strconv.FormatInt(l.Amount, 10),
			l.Currency, l.MerchantID, l.ClearedAt.UTC().Format(time.RFC3339)})
	}
	w.Flush()
	return buf.Bytes()
}

func importLines(tb testing.TB, lines ...Line) Summary {
	tb.Helper()
	s, err := Import(context.Background(), testPool, fileOf(lines...), testDate, testConfig)
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

type record struct {
	status                  string
	fromHeld, fromAvailable int64
	released, shortfall     int64
	credited                int64
	flag                    string
	hasLedger               bool
}

func loadRecord(tb testing.TB, clearingID string) record {
	tb.Helper()
	var r record
	if err := testPool.QueryRow(context.Background(), `
		SELECT status, charged_from_held, charged_from_available, released_amount, shortfall,
			credited_amount, COALESCE(flag, ''), ledger_txn_id IS NOT NULL
		FROM clearing_records WHERE clearing_id = $1`, clearingID).
		Scan(&r.status, &r.fromHeld, &r.fromAvailable, &r.released, &r.shortfall, &r.credited, &r.flag, &r.hasLedger); err != nil {
		tb.Fatalf("record %s: %v", clearingID, err)
	}
	return r
}

type authState struct {
	held, released, cleared int64
	clearedAt               bool
}

func loadAuth(tb testing.TB, authID string) authState {
	tb.Helper()
	var a authState
	if err := testPool.QueryRow(context.Background(), `
		SELECT held_amount, released_amount, cleared_amount, cleared_at IS NOT NULL
		FROM card_authorizations WHERE auth_id = $1`, authID).Scan(&a.held, &a.released, &a.cleared, &a.clearedAt); err != nil {
		tb.Fatalf("authorization %s: %v", authID, err)
	}
	return a
}

func storedBalance(tb testing.TB, accountID string) int64 {
	tb.Helper()
	balance, _, err := ledger.StoredBalance(context.Background(), testPool, accountID)
	if err != nil {
		tb.Fatal(err)
	}
	return balance
}

// payable is what we owe the network: a settlement account, derived from entries.
func payable(tb testing.TB) int64 {
	tb.Helper()
	balance, err := ledger.Balance(context.Background(), testPool, NetworkPayableAccountID)
	if err != nil {
		tb.Fatal(err)
	}
	return balance
}

func dailySpend(tb testing.TB, cardID string) int64 {
	tb.Helper()
	var n int64
	if err := testPool.QueryRow(context.Background(),
		`SELECT COALESCE(sum(amount), 0)::bigint FROM card_daily_spend WHERE card_id = $1`, cardID).Scan(&n); err != nil {
		tb.Fatal(err)
	}
	return n
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

// moneyState is everything a replay must leave untouched.
func moneyState(tb testing.TB) string {
	tb.Helper()
	var s string
	if err := testPool.QueryRow(context.Background(), `
		SELECT (SELECT count(*) FROM ledger_transactions) || '/' ||
			(SELECT count(*) FROM ledger_entries) || '/' ||
			(SELECT count(*) FROM outbox_events) || '/' ||
			(SELECT count(*) FROM clearing_records) || '/' ||
			(SELECT count(*) FROM clearing_files) || '/' ||
			(SELECT COALESCE(string_agg(account_id || '=' || balance, ',' ORDER BY account_id), '') FROM account_balances) || '/' ||
			(SELECT COALESCE(string_agg(auth_id || '=' || held_amount || ':' || cleared_amount, ',' ORDER BY auth_id), '') FROM card_authorizations)`).
		Scan(&s); err != nil {
		tb.Fatal(err)
	}
	return s
}
