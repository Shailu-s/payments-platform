package cardrecon

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/cards"
	"github.com/Shailu-s/payments-platform/internal/clearing"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testSchema = "test_cardrecon"

var (
	testPool   *pgxpool.Pool
	testDate   = time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	testConfig = Config{HoldExpiryDays: 7}
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

func newID(prefix string) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

// newCard issues a card on a fresh funding account holding balance.
func newCard(tb testing.TB, balance int64) cards.Card {
	tb.Helper()
	ctx := context.Background()
	_, key, err := auth.Generate("card recon test")
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
	if _, err := ledger.Record(ctx, testPool, "funding", []ledger.Entry{
		{AccountID: "acc_settlement_usd", Direction: ledger.DirectionDebit, Amount: balance},
		{AccountID: funding, Direction: ledger.DirectionCredit, Amount: balance},
	}); err != nil {
		tb.Fatal(err)
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

// placeHold stores an approved authorization of amount holding amount -
// shortfall, as the authorization path (or a stand-in advice) does.
func placeHold(tb testing.TB, card cards.Card, authID string, amount, shortfall int64) {
	tb.Helper()
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer tx.Rollback(ctx)
	held := amount - shortfall
	var txnID *string
	if held > 0 {
		id := ledger.NewTxnID()
		txnID = &id
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO card_authorizations (auth_id, card_id, account_id, held_account_id, amount,
			currency, merchant_id, merchant_name, mcc, kind, decision, approved_amount, stand_in,
			hold_shortfall, ledger_txn_id, request_fingerprint, held_amount)
		VALUES ($1, $2, $3, $4, $5, 'USD', 'mer_test', 'Test', '5814', 'initial', 'approved', $5, $6 > 0,
			$6, $7, 'test', $8)`,
		authID, card.ID, card.AccountID, card.HeldAccountID, amount, shortfall, txnID, held); err != nil {
		tb.Fatal(err)
	}
	if txnID != nil {
		if err := ledger.RecordWithID(ctx, tx, *txnID, "authorization "+authID, []ledger.Entry{
			{AccountID: card.AccountID, Direction: ledger.DirectionDebit, Amount: held},
			{AccountID: card.HeldAccountID, Direction: ledger.DirectionCredit, Amount: held},
		}); err != nil {
			tb.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		tb.Fatal(err)
	}
}

// releasedAuth stores an approved authorization already released in full,
// by a reversal or, with expiredAt set, by expiry. No money is held.
func releasedAuth(tb testing.TB, card cards.Card, authID string, amount int64, expiredAt *time.Time) {
	tb.Helper()
	if _, err := testPool.Exec(context.Background(), `
		INSERT INTO card_authorizations (auth_id, card_id, account_id, held_account_id, amount,
			currency, merchant_id, merchant_name, mcc, kind, decision, approved_amount,
			request_fingerprint, held_amount, released_amount, expired_at)
		VALUES ($1, $2, $3, $4, $5, 'USD', 'mer_test', 'Test', '5814', 'initial', 'approved', $5,
			'test', 0, $5, $6)`,
		authID, card.ID, card.AccountID, card.HeldAccountID, amount, expiredAt); err != nil {
		tb.Fatal(err)
	}
}

// raise approves an incremental authorization of increase on original, as
// the authorization path does: a new row, the chain's hold raised on the
// original, and the ledger move.
func raise(tb testing.TB, card cards.Card, original, authID string, increase int64) {
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
			currency, merchant_id, merchant_name, mcc, kind, original_auth_id, decision, approved_amount,
			ledger_txn_id, request_fingerprint)
		VALUES ($1, $2, $3, $4, $5, 'USD', 'mer_test', 'Test', '5814', 'incremental', $6, 'approved', $5, $7, 'test')`,
		authID, card.ID, card.AccountID, card.HeldAccountID, increase, original, txnID); err != nil {
		tb.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE card_authorizations SET approved_amount = approved_amount + $2,
		held_amount = held_amount + $2 WHERE auth_id = $1`, original, increase); err != nil {
		tb.Fatal(err)
	}
	if err := ledger.RecordWithID(ctx, tx, txnID, "authorization "+authID, []ledger.Entry{
		{AccountID: card.AccountID, Direction: ledger.DirectionDebit, Amount: increase},
		{AccountID: card.HeldAccountID, Direction: ledger.DirectionCredit, Amount: increase},
	}); err != nil {
		tb.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		tb.Fatal(err)
	}
}

func purchase(authID, cardID string, amount int64) clearing.Line {
	return clearing.Line{
		ClearingID: newID("clr"), AuthID: authID, CardID: cardID, Kind: clearing.KindPurchase, Amount: amount,
		Currency: clearing.Currency, MerchantID: "mer_test", ClearedAt: testDate.Add(10 * time.Hour),
	}
}

func refund(authID, cardID string, amount int64) clearing.Line {
	l := purchase(authID, cardID, amount)
	l.Kind = clearing.KindRefund
	return l
}

func importLines(tb testing.TB, lines ...clearing.Line) {
	tb.Helper()
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write([]string{"clearing_id", "auth_id", "card_id", "kind", "amount", "currency", "merchant_id", "cleared_at"})
	for _, l := range lines {
		_ = w.Write([]string{l.ClearingID, l.AuthID, l.CardID, l.Kind, strconv.FormatInt(l.Amount, 10),
			l.Currency, l.MerchantID, l.ClearedAt.UTC().Format(time.RFC3339)})
	}
	w.Flush()
	if _, err := clearing.Import(context.Background(), testPool, buf.Bytes(), testDate, clearing.Config{OverTolerancePct: 20}); err != nil {
		tb.Fatal(err)
	}
}

// statementOf is the network's statement for lines, every one of them.
func statementOf(date string, lines ...clearing.Line) []byte {
	var purchases, refunds int64
	for _, l := range lines {
		if l.Kind == clearing.KindRefund {
			refunds += l.Amount
		} else {
			purchases += l.Amount
		}
	}
	return statement(date, purchases, refunds, len(lines))
}

func statement(date string, purchases, refunds int64, count int) []byte {
	return fmt.Appendf(nil, "settlement_date,purchases_total,refunds_total,net_amount,line_count,currency\n%s,%d,%d,%d,%d,USD\n",
		date, purchases, refunds, purchases-refunds, count)
}

func reconcile(tb testing.TB, id string, raw []byte) Run {
	tb.Helper()
	run, err := Reconcile(context.Background(), testPool, id, bytes.NewReader(raw), testConfig)
	if err != nil {
		tb.Fatal(err)
	}
	return run
}

func allFindings(tb testing.TB, id string) []Finding {
	tb.Helper()
	var all []Finding
	for offset := 0; ; offset += 100 {
		page, err := Findings(context.Background(), testPool, id, "", 100, offset)
		if err != nil {
			tb.Fatal(err)
		}
		all = append(all, page...)
		if len(page) < 100 {
			return all
		}
	}
}

// moneyState is everything a reconciliation must leave untouched.
func moneyState(tb testing.TB) string {
	tb.Helper()
	var s string
	if err := testPool.QueryRow(context.Background(), `
		SELECT (SELECT count(*) FROM ledger_transactions) || '/' ||
			(SELECT count(*) FROM ledger_entries) || '/' ||
			(SELECT count(*) FROM outbox_events) || '/' ||
			(SELECT count(*) FROM network_settlements) || '/' ||
			(SELECT COALESCE(string_agg(account_id || '=' || balance, ',' ORDER BY account_id), '') FROM account_balances) || '/' ||
			(SELECT COALESCE(string_agg(clearing_id || '=' || status || ':' || COALESCE(flag, ''), ',' ORDER BY clearing_id), '') FROM clearing_records) || '/' ||
			(SELECT COALESCE(string_agg(auth_id || '=' || held_amount || ':' || released_amount || ':' || cleared_amount || ':' || refunded_amount, ',' ORDER BY auth_id), '') FROM card_authorizations)`).
		Scan(&s); err != nil {
		tb.Fatal(err)
	}
	return s
}

func countWhere(tb testing.TB, q string, args ...any) int {
	tb.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		tb.Fatalf("%s: %v", q, err)
	}
	return n
}
