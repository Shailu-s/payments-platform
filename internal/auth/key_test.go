package auth

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// The constraint the whole package exists to satisfy: a stolen database dump
// must not yield working keys. Nothing stored may resemble the plaintext.
func TestStoredKeyDoesNotContainThePlaintext(t *testing.T) {
	plaintext, key, err := Generate("dump test")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	secret := strings.TrimPrefix(plaintext, keyPrefix)
	if strings.Contains(key.Hash, secret) {
		t.Error("the stored hash contains the plaintext secret")
	}
	if key.Hash == plaintext {
		t.Error("the stored hash is the plaintext")
	}
	// The prefix must identify a key without being usable as one. It carries 8
	// characters of the secret, leaving 35 unknown — brute forcing those is not
	// a threat model, and a prefix of only "pk_live_" would identify nothing.
	if !strings.HasPrefix(plaintext, key.Prefix) {
		t.Errorf("Prefix %q is not a prefix of the key", key.Prefix)
	}
	if key.Prefix == keyPrefix {
		t.Error("the prefix is just the marker, so it cannot identify a key")
	}
	if key.Prefix == plaintext {
		t.Error("the prefix is the whole key")
	}
	if len(key.Prefix) >= len(plaintext) {
		t.Errorf("prefix length %d leaves nothing secret", len(key.Prefix))
	}
	if strings.Contains(key.Hash, key.Prefix) {
		t.Error("the stored hash leaks the prefix")
	}
}

func TestGenerateProducesDistinctUnpredictableKeys(t *testing.T) {
	const runs = 1000
	seen := make(map[string]bool, runs)
	for i := 0; i < runs; i++ {
		plaintext, key, err := Generate("uniqueness")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		if seen[plaintext] {
			t.Fatalf("Generate returned a duplicate key after %d calls", i)
		}
		seen[plaintext] = true

		if !strings.HasPrefix(plaintext, keyPrefix) {
			t.Fatalf("plaintext %q does not start with %q", plaintext, keyPrefix)
		}
		// 32 random bytes as unpadded base64url is 43 characters.
		if got := len(plaintext) - len(keyPrefix); got != 43 {
			t.Fatalf("secret length = %d, want 43", got)
		}
		if key.Hash == "" {
			t.Fatal("Generate returned an empty hash")
		}
	}
}

func TestGenerateRejectsAnEmptyName(t *testing.T) {
	if _, _, err := Generate("   "); err == nil {
		t.Error("Generate accepted a blank name, want an error")
	}
}

func TestVerifyAcceptsALiveKey(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	plaintext, created := mustGenerate(t, "live key")

	got, err := Verify(ctx, testPool, plaintext)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("Verify returned id %q, want %q", got.ID, created.ID)
	}
	if got.Name != "live key" {
		t.Errorf("Verify returned name %q, want %q", got.Name, "live key")
	}
	if got.RevokedAt != nil {
		t.Error("a live key came back with a revocation timestamp")
	}
}

func TestVerifyRejectsUnknownAndMalformedKeys(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	valid, _ := mustGenerate(t, "the real key")

	// A well-formed key that was never issued.
	unissued, _, err := Generate("never inserted")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	tests := []struct {
		name string
		key  string
	}{
		{"empty", ""},
		{"no prefix", strings.TrimPrefix(valid, keyPrefix)},
		{"wrong prefix", "sk_test_" + strings.TrimPrefix(valid, keyPrefix)},
		{"truncated", valid[:len(valid)-1]},
		{"extra character", valid + "a"},
		{"one character changed", flipLastChar(valid)},
		{"well formed but never issued", unissued},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Verify(ctx, testPool, tc.key); !errors.Is(err, ErrInvalidKey) {
				t.Fatalf("Verify(%q) error = %v, want ErrInvalidKey", tc.key, err)
			}
		})
	}
}

// Revocation is a timestamp, not a DELETE: transfers reference the key that
// authorised them and the audit trail has to survive.
func TestVerifyRejectsARevokedKeyButKeepsTheRow(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	plaintext, created := mustGenerate(t, "to be revoked")

	if _, err := Verify(ctx, testPool, plaintext); err != nil {
		t.Fatalf("Verify before revocation: %v", err)
	}
	if err := Revoke(ctx, testPool, created.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	if _, err := Verify(ctx, testPool, plaintext); !errors.Is(err, ErrRevokedKey) {
		t.Fatalf("Verify after revocation error = %v, want ErrRevokedKey", err)
	}

	var count int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM api_keys WHERE id = $1`, created.ID).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("revoked key row count = %d, want 1: revocation must not delete the row", count)
	}
}

func TestRevokeIsNotSilentlyRepeatable(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	_, created := mustGenerate(t, "double revoke")

	if err := Revoke(ctx, testPool, created.ID); err != nil {
		t.Fatalf("first Revoke: %v", err)
	}
	if err := Revoke(ctx, testPool, created.ID); !errors.Is(err, ErrInvalidKey) {
		t.Errorf("second Revoke error = %v, want ErrInvalidKey", err)
	}
	if err := Revoke(ctx, testPool, "ak_does_not_exist"); !errors.Is(err, ErrInvalidKey) {
		t.Errorf("Revoke of an unknown id error = %v, want ErrInvalidKey", err)
	}
}

// The database enforces uniqueness of the hash, not application code.
func TestInsertRejectsADuplicateHash(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	_, key := mustGenerate(t, "original")

	duplicate := key
	duplicate.ID = newID("ak")
	if err := Insert(ctx, testPool, duplicate); err == nil {
		t.Error("Insert accepted a duplicate key hash, want a unique violation")
	}
}

func flipLastChar(s string) string {
	if s == "" {
		return s
	}
	last := s[len(s)-1]
	if last == 'A' {
		return s[:len(s)-1] + "B"
	}
	return s[:len(s)-1] + "A"
}
