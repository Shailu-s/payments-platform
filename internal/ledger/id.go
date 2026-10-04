package ledger

import (
	"crypto/rand"
	"encoding/hex"
)

// Generate unpredictable IDs before insertion so related writes can reference them.
func newID(prefix string) string {
	var b [16]byte
	// Go 1.24+ rand.Read fills the buffer or terminates on entropy failure.
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}
