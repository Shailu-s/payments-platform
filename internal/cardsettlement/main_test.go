package cardsettlement

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

const testSchema = "test_cardsettlement"

var (
	testPool *pgxpool.Pool
	testDate = time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
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
	_, key, err := auth.Generate("settlement test")
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

// line is a clearing line on testDate with no authorization: the account is
// charged (purchase) or credited (refund) directly.
func line(kind, cardID string, amount int64) clearing.Line {
	return clearing.Line{
		ClearingID: newID("clr"), CardID: cardID, Kind: kind, Amount: amount,
		Currency: clearing.Currency, MerchantID: "mer_test", ClearedAt: testDate.Add(10 * time.Hour),
	}
}

func importLines(tb testing.TB, date time.Time, lines ...clearing.Line) {
	tb.Helper()
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write([]string{"clearing_id", "auth_id", "card_id", "kind", "amount", "currency", "merchant_id", "cleared_at"})
	for _, l := range lines {
		_ = w.Write([]string{l.ClearingID, l.AuthID, l.CardID, l.Kind, strconv.FormatInt(l.Amount, 10),
			l.Currency, l.MerchantID, l.ClearedAt.UTC().Format(time.RFC3339)})
	}
	w.Flush()
	if _, err := clearing.Import(context.Background(), testPool, buf.Bytes(), date, clearing.Config{OverTolerancePct: 20}); err != nil {
		tb.Fatal(err)
	}
}

func balance(tb testing.TB, accountID string) int64 {
	tb.Helper()
	b, err := ledger.Balance(context.Background(), testPool, accountID)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

func countWhere(tb testing.TB, q string, args ...any) int {
	tb.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		tb.Fatalf("%s: %v", q, err)
	}
	return n
}
