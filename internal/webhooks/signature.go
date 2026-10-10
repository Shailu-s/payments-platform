package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

const (
	TimestampHeader = "X-MockBank-Timestamp"
	SignatureHeader = "X-MockBank-Signature"
	MinSecretBytes  = 32
	ReplayWindow    = 5 * time.Minute
)

func Sign(secret []byte, timestamp string, body []byte) string {
	return "v1=" + hex.EncodeToString(digest(secret, timestamp, body))
}

func Verify(secret []byte, timestamp, signature string, body []byte, now time.Time) bool {
	if len(secret) < MinSecretBytes || len(timestamp) == 0 || len(timestamp) > 20 ||
		len(signature) != 3+sha256.Size*2 || !strings.HasPrefix(signature, "v1=") {
		return false
	}
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	window := int64(ReplayWindow / time.Second)
	if err != nil || seconds < now.Unix()-window || seconds > now.Unix()+window {
		return false
	}
	provided, err := hex.DecodeString(signature[3:])
	return err == nil && hmac.Equal(provided, digest(secret, timestamp, body))
}

func digest(secret []byte, timestamp string, body []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(body)
	return mac.Sum(nil)
}
