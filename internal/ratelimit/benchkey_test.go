package ratelimit

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

// A minimal key row for benchmarks, without importing the auth package's
// full generation path.
type benchKeyRow struct{ id, hash, prefix string }

func generateBenchKey() (string, benchKeyRow, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", benchKeyRow{}, err
	}
	plaintext := "pk_live_" + hex.EncodeToString(b[:])
	sum := sha256.Sum256([]byte(plaintext))

	var idBytes [16]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return "", benchKeyRow{}, err
	}
	return plaintext, benchKeyRow{
		id:     "ak_" + hex.EncodeToString(idBytes[:]),
		hash:   hex.EncodeToString(sum[:]),
		prefix: plaintext[:16],
	}, nil
}
