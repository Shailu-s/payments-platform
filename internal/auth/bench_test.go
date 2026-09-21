package auth

import (
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// The number behind the hashing decision, measured rather than asserted.
//
// Slow hashes exist because passwords are low-entropy and guessable: making
// each attacker guess cost ~100ms turns a billion guesses into years. An api
// key is 256 bits of crypto/rand, so there is no dictionary to try and nothing
// to slow down — while the cost lands on every single request.
//
//	go test ./internal/auth/ -bench=Hash -benchtime=100x -run=XXX
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
