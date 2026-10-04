package auth

import (
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// Compare hashing costs for random API keys, which do not need password-style slow hashing.
func BenchmarkSHA256Hash(b *testing.B) {
	plaintext, _, err := Generate("benchmark")
	if err != nil {
		b.Fatalf("Generate: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hashKey(plaintext)
	}
}

func BenchmarkBcryptHash(b *testing.B) {
	plaintext, _, err := Generate("benchmark")
	if err != nil {
		b.Fatalf("Generate: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost); err != nil {
			b.Fatalf("bcrypt: %v", err)
		}
	}
}

// Verification is what actually runs on every request, so measure that too:
// bcrypt cannot look a key up by hash, it has to compare against a candidate.
func BenchmarkBcryptCompare(b *testing.B) {
	plaintext, _, err := Generate("benchmark")
	if err != nil {
		b.Fatalf("Generate: %v", err)
	}
	hashed, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
	if err != nil {
		b.Fatalf("bcrypt: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bcrypt.CompareHashAndPassword(hashed, []byte(plaintext)); err != nil {
			b.Fatalf("compare: %v", err)
		}
	}
}
