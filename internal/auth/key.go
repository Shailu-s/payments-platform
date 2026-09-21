// Package auth issues and verifies API keys.
//
// V1 has authentication but not authorisation: a valid key identifies the
// caller and is recorded against every transfer, but any valid key may act on
// any account. Multi-tenancy is a deliberate omission, recorded in PLAN.md.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Plaintext keys look like "pk_live_" + 43 base64url characters.
const (
	keyPrefix = "pk_live_"
	// 32 bytes, 256 bits. Far beyond brute force, which is what makes a fast
	// hash the right choice below.
	keyBytes = 32
	// Characters of the secret kept in the prefix column, after the "pk_live_"
	// marker. Eight characters of the literal key would be "pk_live_" itself,
	// which is identical for every key and identifies nothing; these are the
	// first 8 of the random material instead, so a key is recognisable in a log
	// line while 35 characters of secret remain unknown.
	prefixLen = 8
)

var (
	ErrInvalidKey = errors.New("api key is not valid")
	ErrRevokedKey = errors.New("api key has been revoked")
)

// Key is an api_keys row. It never holds the plaintext: after Generate returns,
// the only copy is the string handed to the caller.
type Key struct {
	ID        string
	Hash      string
	Prefix    string
	Name      string
	CreatedAt time.Time
	RevokedAt *time.Time
}

// Generate mints a key. The plaintext is returned once and is unrecoverable
// afterwards, because only its hash is ever written down: a stolen database
// dump must not yield working keys.
func Generate(name string) (plaintext string, key Key, err error) {
	if strings.TrimSpace(name) == "" {
		return "", Key{}, errors.New("api key needs a name")
	}

	var secret [keyBytes]byte
	// Since Go 1.24 crypto/rand.Read never returns an error: it panics
	// internally if the entropy source fails rather than returning a short
	// read. Discarded explicitly so that reads as a decision.
	_, _ = rand.Read(secret[:])

	plaintext = keyPrefix + base64.RawURLEncoding.EncodeToString(secret[:])

	return plaintext, Key{
		ID:     newID("ak"),
		Hash:   hashKey(plaintext),
		Prefix: keyPrefix + plaintext[len(keyPrefix):len(keyPrefix)+prefixLen],
		Name:   name,
	}, nil
}

// Insert writes a generated key. Separate from Generate so that minting is
// pure and testable without a database.
func Insert(ctx context.Context, db Execer, key Key) error {
	const q = `INSERT INTO api_keys (id, key_hash, prefix, name) VALUES ($1, $2, $3, $4)`
	if _, err := db.Exec(ctx, q, key.ID, key.Hash, key.Prefix, key.Name); err != nil {
		return fmt.Errorf("insert api key %s: %w", key.ID, err)
	}
	return nil
}

// Verify resolves a plaintext key to its row. It returns ErrInvalidKey for
// anything unknown and ErrRevokedKey for a key that existed and was withdrawn
// — the caller maps both to 401, but the distinction matters in a log line.
func Verify(ctx context.Context, db Querier, plaintext string) (Key, error) {
	// Cheap shape check first, so a malformed header never reaches the database.
	if !strings.HasPrefix(plaintext, keyPrefix) || len(plaintext) != len(keyPrefix)+43 {
		return Key{}, ErrInvalidKey
	}

	const q = `
		SELECT id, key_hash, prefix, name, created_at, revoked_at
		FROM api_keys
		WHERE key_hash = $1`

	// Lookup is by hash, not by prefix: the hash is UNIQUE and indexed, and the
	// prefix is not a secret and is not unique.
	var key Key
	err := db.QueryRow(ctx, q, hashKey(plaintext)).
		Scan(&key.ID, &key.Hash, &key.Prefix, &key.Name, &key.CreatedAt, &key.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Key{}, ErrInvalidKey
	}
	if err != nil {
		return Key{}, fmt.Errorf("verify api key: %w", err)
	}

	// The row was found by its hash, so this can only fail if the database
	// returned something other than what was asked for. Constant time anyway:
	// comparison of secret material should not be the one place that leaks.
	if subtle.ConstantTimeCompare([]byte(key.Hash), []byte(hashKey(plaintext))) != 1 {
		return Key{}, ErrInvalidKey
	}

	if key.RevokedAt != nil {
		return Key{}, ErrRevokedKey
	}
	return key, nil
}

// Revoke withdraws a key. A timestamp rather than a DELETE, because transfers
// reference the key that authorised them and the audit trail has to survive.
func Revoke(ctx context.Context, db Execer, id string) error {
	const q = `UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`
	tag, err := db.Exec(ctx, q, id)
	if err != nil {
		return fmt.Errorf("revoke api key %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("revoke api key %s: %w", id, ErrInvalidKey)
	}
	return nil
}

// hashKey is SHA-256, deliberately, and this is the decision to be able to
// defend. bcrypt and argon2 are slow on purpose because passwords are
// low-entropy and guessable. An api key is 256 bits of crypto/rand and cannot
// be brute-forced, so the slow-hash argument does not apply — while bcrypt on
// every request would add tens of milliseconds to the hot path of every call.
func hashKey(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}
