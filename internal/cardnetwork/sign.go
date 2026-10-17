package cardnetwork

import (
	"net/http"
	"time"

	"github.com/Shailu-s/payments-platform/internal/webhooks"
)

const (
	TimestampHeader = "X-Network-Timestamp"
	SignatureHeader = "X-Network-Signature"
)

// VerifyRequest checks a network request's signature over its exact raw body, with
// the same scheme and replay window as MockBank webhooks.
func VerifyRequest(secret []byte, h http.Header, body []byte, now time.Time) bool {
	return webhooks.Verify(secret, h.Get(TimestampHeader), h.Get(SignatureHeader), body, now)
}
