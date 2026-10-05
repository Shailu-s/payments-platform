// Package auth issues and verifies API keys.
//
// Authentication only: any valid key may act on any account.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
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

	keyBytes = 32
	// Count random characters, not the common pk_live_ marker, for identification.
	prefixLen = 8
)

var (
	ErrInvalidKey = errors.New("api key is not valid")
	ErrRevokedKey = errors.New("api key has been revoked")
)

// Key holds the stored hash and metadata, never the plaintext.
type Key struct {
	ID        string
	Hash      string
	Prefix    string
	Name      string
	CreatedAt time.Time
	RevokedAt *time.Time
}

// Generate returns plaintext once and a hash-only record for storage.
func Generate(name string) (plaintext string, key Key, err error) {
	if strings.TrimSpace(name) == "" {
		return "", Key{}, errors.New("api key needs a name")
	}

	var secret [keyBytes]byte
	// Go 1.24+ rand.Read fills the buffer or terminates on entropy failure.
	_, _ = rand.Read(secret[:])

	plaintext = keyPrefix + base64.RawURLEncoding.EncodeToString(secret[:])

	return plaintext, Key{
		ID:     newID("ak"),
		Hash:   hashKey(plaintext),
		Prefix: keyPrefix + plaintext[len(keyPrefix):len(keyPrefix)+prefixLen],
		Name:   name,
	}, nil
}

// Insert stores a generated key's hash and metadata.
func Insert(ctx context.Context, db Execer, key Key) error {
	const q = `INSERT INTO api_keys (id, key_hash, prefix, name) VALUES ($1, $2, $3, $4)`
	if _, err := db.Exec(ctx, q, key.ID, key.Hash, key.Prefix, key.Name); err != nil {
		return fmt.Errorf("insert api key %s: %w", key.ID, err)
	}
	return nil
}

// Verify resolves a key by its hash, returning ErrInvalidKey or ErrRevokedKey.
// It performs an exact hash lookup, not a byte-by-byte plaintext comparison.
func Verify(ctx context.Context, db Querier, plaintext string) (Key, error) {
	// Cheap shape check first, so a malformed header never reaches the database.
	if !strings.HasPrefix(plaintext, keyPrefix) || len(plaintext) != len(keyPrefix)+43 {
		return Key{}, ErrInvalidKey
	}

	const q = `
		SELECT id, key_hash, prefix, name, created_at, revoked_at
		FROM api_keys
		WHERE key_hash = $1`

	// The display prefix is neither secret nor unique.
	var key Key
	err := db.QueryRow(ctx, q, hashKey(plaintext)).
		Scan(&key.ID, &key.Hash, &key.Prefix, &key.Name, &key.CreatedAt, &key.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Key{}, ErrInvalidKey
	}
	if err != nil {
		return Key{}, fmt.Errorf("verify api key: %w", err)
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

// Random 256-bit keys do not need password-style slow hashing on every request.
func hashKey(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}
