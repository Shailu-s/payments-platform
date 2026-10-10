package api

import (
	"context"
	"net/http"
	"testing"
)

func TestEveryFinancialTransitionRetainsAuditEvidence(t *testing.T) {
	for _, status := range []string{"settled", "failed"} {
		t.Run(status, func(t *testing.T) {
			ctx := context.Background()
			h, key := newTestServer(t)
			id, _, _ := sentTransfer(t, h, key, "mb_audit")
			event := eventFor("evt_audit", "mb_audit", id, status)
			if response := sendWebhook(h, event); response.Code != http.StatusOK {
				t.Fatalf("callback=%d %s", response.Code, response.Body.String())
			}
			var caller, storedStatus, fingerprint, initialTransaction string
			if err := testPool.QueryRow(ctx, `SELECT api_key_id,status,request_fingerprint,ledger_txn_id FROM transfers WHERE id=$1`, id).Scan(&caller, &storedStatus, &fingerprint, &initialTransaction); err != nil || caller == "" || fingerprint == "" || initialTransaction == "" || storedStatus != status {
				t.Fatalf("intent audit missing: caller=%q fingerprint=%q transaction=%q status=%q err=%v", caller, fingerprint, initialTransaction, storedStatus, err)
			}
			var creation, terminal, events, callbacks int
			prefix := "settlement "
			if status == "failed" {
				prefix = "reversal "
			}
			if err := testPool.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM ledger_transactions WHERE id=$2 AND reference='transfer '||$1),
				(SELECT count(*) FROM ledger_transactions WHERE reference=$3||$1),
				(SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type IN ('transfer.created',$4)),
				(SELECT count(*) FROM webhook_events WHERE event_id='evt_audit')`, id, initialTransaction, prefix, "transfer."+status).Scan(&creation, &terminal, &events, &callbacks); err != nil || creation != 1 || terminal != 1 || events != 2 || callbacks != 1 {
				t.Fatalf("financial audit: creation=%d terminal=%d events=%d callbacks=%d err=%v", creation, terminal, events, callbacks, err)
			}
			var entries int
			if err := testPool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries WHERE txn_id IN (SELECT id FROM ledger_transactions WHERE reference IN ('transfer '||$1,$2||$1))`, id, prefix).Scan(&entries); err != nil || entries != 4 {
				t.Fatalf("audit entries=%d err=%v", entries, err)
			}
		})
	}
}
