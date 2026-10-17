// Package cards stores cards and the card accounts they spend from.
//
// A card is a handle with a status and limits; the money sits in ledger
// accounts. Each funding account gets one held account, shared by its cards,
// for money set aside by authorizations that have not cleared yet.
package cards

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	StatusActive = "active"
	StatusFrozen = "frozen"
	// StatusClosed is terminal.
	StatusClosed = "closed"
)

var (
	ErrNotFound = errors.New("card not found")
	// ErrDuplicateKey means another card owns the idempotency key.
	ErrDuplicateKey = errors.New("idempotency key already used")
)

type Card struct {
	ID                  string
	AccountID           string
	HeldAccountID       string
	Last4               string
	Status              string
	PerTransactionLimit int64
	DailyLimit          int64
	BlockedMCCs         []string
	APIKeyID            string
	IdempotencyKey      string
	RequestFingerprint  string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// Querier allows reads to share the caller's transaction.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Beginner lets a card change and its outbox event share a transaction.
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

const columns = `c.id, c.account_id, ca.held_account_id, c.last4, c.status,
	c.per_transaction_limit, c.daily_limit, c.blocked_mccs, c.api_key_id, c.idempotency_key,
	c.request_fingerprint, c.created_at, c.updated_at`

// Issue creates an active card for c.AccountID, and the account's held
// account if this is its first card, with a card.issued event, atomically.
// The caller chooses the card id and validates the funding account.
func Issue(ctx context.Context, db Beginner, c Card) (Card, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return Card{}, fmt.Errorf("begin issue %s: %w", c.ID, err)
	}
	defer tx.Rollback(ctx)

	held, err := ensureHeldAccount(ctx, tx, c.AccountID)
	if err != nil {
		return Card{}, err
	}

	last4, err := randomLast4()
	if err != nil {
		return Card{}, err
	}

	// The unique index on the idempotency key picks one winner among
	// concurrent retries; the losers wait here for it to commit.
	const q = `
		INSERT INTO cards (id, account_id, last4, status, per_transaction_limit,
			daily_limit, blocked_mccs, api_key_id, idempotency_key, request_fingerprint)
		VALUES ($1, $2, $3, 'active', $4, $5, $6, $7, $8, $9)
		RETURNING created_at, updated_at`
	if c.BlockedMCCs == nil {
		// nil would be sent as NULL, which the column refuses.
		c.BlockedMCCs = []string{}
	}
	err = tx.QueryRow(ctx, q, c.ID, c.AccountID, last4, c.PerTransactionLimit,
		c.DailyLimit, c.BlockedMCCs, c.APIKeyID, c.IdempotencyKey, c.RequestFingerprint,
	).Scan(&c.CreatedAt, &c.UpdatedAt)
	if isUniqueViolation(err) {
		return Card{}, ErrDuplicateKey
	}
	if err != nil {
		return Card{}, fmt.Errorf("insert card %s: %w", c.ID, err)
	}
	c.HeldAccountID, c.Last4, c.Status = held, last4, StatusActive

	if err := insertEvent(ctx, tx, "card.issued", c); err != nil {
		return Card{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Card{}, fmt.Errorf("commit issue %s: %w", c.ID, err)
	}
	return c, nil
}

// ensureHeldAccount returns the funding account's held account, creating it
// on the first card. Two first cards issued concurrently must share one.
//
// The held account is inserted only by the statement that wins the
// card_accounts row, so the loser creates nothing: no orphan account, and
// no orphan balance row from the accounts trigger. The foreign key from
// card_accounts to the held account is checked at the end of the statement,
// after both inserts.
func ensureHeldAccount(ctx context.Context, tx pgx.Tx, accountID string) (string, error) {
	const claim = `
		WITH link AS (
			INSERT INTO card_accounts (account_id, held_account_id) VALUES ($1, $2)
			ON CONFLICT (account_id) DO NOTHING
			RETURNING held_account_id
		)
		INSERT INTO accounts (id, currency, type)
		SELECT held_account_id, currency, 'asset'
		FROM link, accounts funding WHERE funding.id = $1
		RETURNING id`

	var held string
	err := tx.QueryRow(ctx, claim, accountID, newID("acc")).Scan(&held)
	if err == nil {
		return held, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("create held account for %s: %w", accountID, err)
	}

	// The conflict waited for the winner to commit, so a new statement sees it.
	const read = `SELECT held_account_id FROM card_accounts WHERE account_id = $1`
	if err := tx.QueryRow(ctx, read, accountID).Scan(&held); err != nil {
		return "", fmt.Errorf("read held account for %s: %w", accountID, err)
	}
	return held, nil
}

// FindByIdempotencyKey returns the card owning a key.
// This lookup alone cannot prevent concurrent inserts; the unique index does.
func FindByIdempotencyKey(ctx context.Context, db Querier, key string) (Card, error) {
	const q = `SELECT ` + columns + `
		FROM cards c JOIN card_accounts ca ON ca.account_id = c.account_id
		WHERE c.idempotency_key = $1`

	c, err := scan(db.QueryRow(ctx, q, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return Card{}, ErrNotFound
	}
	if err != nil {
		return Card{}, fmt.Errorf("find card by idempotency key: %w", err)
	}
	return c, nil
}

func Get(ctx context.Context, db Querier, id string) (Card, error) {
	const q = `SELECT ` + columns + `
		FROM cards c JOIN card_accounts ca ON ca.account_id = c.account_id
		WHERE c.id = $1`

	c, err := scan(db.QueryRow(ctx, q, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Card{}, ErrNotFound
	}
	if err != nil {
		return Card{}, fmt.Errorf("get card %s: %w", id, err)
	}
	return c, nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scan(row scannable) (Card, error) {
	var c Card
	err := row.Scan(&c.ID, &c.AccountID, &c.HeldAccountID, &c.Last4, &c.Status,
		&c.PerTransactionLimit, &c.DailyLimit, &c.BlockedMCCs, &c.APIKeyID, &c.IdempotencyKey,
		&c.RequestFingerprint, &c.CreatedAt, &c.UpdatedAt)
	return c, err
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}

// randomLast4 is cosmetic: there is no card number for it to come from.
func randomLast4() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(10000))
	if err != nil {
		return "", fmt.Errorf("generate last4: %w", err)
	}
	return fmt.Sprintf("%04d", n.Int64()), nil
}

func newID(prefix string) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}
