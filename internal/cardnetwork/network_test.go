package cardnetwork

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestEveryRequestIsSignedAndVerifiable(t *testing.T) {
	issuer := newFakeIssuer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		var a Authorization
		_ = json.Unmarshal(body, &a)
		if r.URL.Path == PathAuthorizations && a.AuthID == "auth_unanswered" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		approveAll(w, r, body)
	})
	clock := NewFakeClock(testStart)
	n := newTestNetwork(t, issuer.srv.URL, clock)
	ctx := context.Background()

	if _, err := n.Authorize(ctx, testAuth("auth_answered", 5000)); err != nil {
		t.Fatal(err)
	}
	res, err := n.Authorize(ctx, testAuth("auth_unanswered", 5000))
	if err != nil || res.Advice != AdviceAcknowledged {
		t.Fatalf("stand-in with advice expected, got %+v, %v", res, err)
	}
	if _, err := n.Reverse(ctx, Reversal{ReversalID: "rev_1", AuthID: "auth_answered", Amount: 5000}); err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}
	for _, req := range issuer.requests() {
		seen[req.Path] = true
		if !VerifyRequest(testSecret, req.Header, req.Body, clock.Now()) {
			t.Errorf("%s: signature does not verify", req.Path)
		}
		tampered := append(append([]byte(nil), req.Body...), ' ')
		if VerifyRequest(testSecret, req.Header, tampered, clock.Now()) {
			t.Errorf("%s: tampered body verifies", req.Path)
		}
		if VerifyRequest([]byte("another-secret-of-at-least-32-bytes!"), req.Header, req.Body, clock.Now()) {
			t.Errorf("%s: verifies under the wrong secret", req.Path)
		}
	}
	for _, path := range []string{PathAuthorizations, PathReversals, PathAdvices} {
		if !seen[path] {
			t.Errorf("no request to %s", path)
		}
	}
}

func TestSlowIssuerGetsOneRetryWithTheSameAuthID(t *testing.T) {
	var calls atomic.Int32
	issuer := newFakeIssuer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if calls.Add(1) == 1 {
			hang(r)
			return
		}
		approveAll(w, r, body)
	})
	n := newTestNetwork(t, issuer.srv.URL, NewFakeClock(testStart))

	res, err := n.Authorize(context.Background(), testAuth("auth_slow", 5000))
	if err != nil {
		t.Fatal(err)
	}
	if res.StandIn || res.Decision != DecisionApproved || res.ApprovedAmount != 5000 || res.Attempts != 2 {
		t.Fatalf("want the issuer's approval on the second attempt, got %+v", res)
	}
	reqs := issuer.requests()
	if len(reqs) != 2 || string(reqs[0].Body) != string(reqs[1].Body) {
		t.Fatalf("want two identical authorization requests, got %d", len(reqs))
	}
	var first Authorization
	_ = json.Unmarshal(reqs[0].Body, &first)
	if first.AuthID != "auth_slow" {
		t.Fatalf("retry changed auth_id: %q", first.AuthID)
	}
	if got := outcomes(n.Journal(), "auth_slow"); !equalStrings(got, []string{OutcomeTimeout, OutcomeAnswered}) {
		t.Fatalf("journal outcomes %v", got)
	}
}

func TestStandInApprovesAfterTwoTimeoutsAndSendsAnAdvice(t *testing.T) {
	issuer := newFakeIssuer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if r.URL.Path == PathAuthorizations {
			hang(r)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	n := newTestNetwork(t, issuer.srv.URL, NewFakeClock(testStart))
	ctx := context.Background()

	res, err := n.Authorize(ctx, testAuth("auth_standin", 5000))
	if err != nil {
		t.Fatal(err)
	}
	if !res.StandIn || res.Decision != DecisionApproved || res.ApprovedAmount != 5000 || res.Advice != AdviceAcknowledged {
		t.Fatalf("want an acknowledged stand-in approval, got %+v", res)
	}
	reqs := issuer.requests()
	if len(reqs) != 3 || reqs[0].Path != PathAuthorizations || reqs[1].Path != PathAuthorizations || reqs[2].Path != PathAdvices {
		t.Fatalf("want auth, auth, advice; got %d requests", len(reqs))
	}
	var advice Advice
	if err := json.Unmarshal(reqs[2].Body, &advice); err != nil {
		t.Fatal(err)
	}
	if advice.AuthID != "auth_standin" || advice.AdviceID != "adv_standin" || advice.Amount != 5000 ||
		advice.Decision != DecisionApproved || advice.Reason != ReasonStandIn || advice.CardID != "card_test" {
		t.Fatalf("advice %+v", advice)
	}

	t.Run("above the stand-in limit is declined without an advice", func(t *testing.T) {
		before := len(issuer.requests())
		res, err := n.Authorize(ctx, testAuth("auth_too_big", 10_001))
		if err != nil {
			t.Fatal(err)
		}
		if !res.StandIn || res.Decision != DecisionDeclined || res.ApprovedAmount != 0 ||
			res.DeclineReason != DeclineStandInLimit || res.Advice != "" {
			t.Fatalf("want a stand-in decline, got %+v", res)
		}
		for _, req := range issuer.requests()[before:] {
			if req.Path == PathAdvices {
				t.Fatal("an advice was sent for a stand-in decline")
			}
		}
	})
}

func TestAdvicesAndReversalsAreRetriedUntilAcknowledged(t *testing.T) {
	var reversalCalls atomic.Int32
	issuer := newFakeIssuer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		switch r.URL.Path {
		case PathReversals:
			if reversalCalls.Add(1) <= 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			// Authorizations and advices fail forever.
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	clock := NewFakeClock(testStart)
	n := newTestNetwork(t, issuer.srv.URL, clock)
	ctx := context.Background()

	acked, err := n.Reverse(ctx, Reversal{ReversalID: "rev_retry", AuthID: "auth_x", Amount: 100})
	if err != nil || !acked {
		t.Fatalf("want the reversal acknowledged on the third attempt, got %v, %v", acked, err)
	}
	if got := outcomes(n.Journal(), "rev_retry"); !equalStrings(got, []string{"status_503", "status_503", OutcomeAcknowledged}) {
		t.Fatalf("reversal outcomes %v", got)
	}
	if waited := clock.Now().Sub(testStart); waited != 3*time.Second {
		t.Fatalf("want 1s + 2s of backoff, clock moved %v", waited)
	}

	res, err := n.Authorize(ctx, testAuth("auth_advice", 5000))
	if err != nil {
		t.Fatal(err)
	}
	if !res.StandIn || res.Advice != AdviceGivenUp {
		t.Fatalf("want a stand-in whose advice was given up, got %+v", res)
	}
	adviceAttempts := 0
	for _, req := range issuer.requests() {
		if req.Path == PathAdvices {
			adviceAttempts++
		}
	}
	if adviceAttempts != len(deliveryBackoff)+1 {
		t.Fatalf("want %d bounded advice attempts, got %d", len(deliveryBackoff)+1, adviceAttempts)
	}
	got := outcomes(n.Journal(), "adv_advice")
	if len(got) != adviceAttempts+1 || got[len(got)-1] != OutcomeGivenUp {
		t.Fatalf("advice outcomes %v", got)
	}
}
