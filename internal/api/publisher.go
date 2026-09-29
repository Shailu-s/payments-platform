package api

import "context"

// TransferEvent is what a consumer needs to act on a transfer without
// re-reading everything about it. Kept small on purpose: an event is a
// notification that something happened, not a copy of the row.
type TransferEvent struct {
	TransferID string `json:"transfer_id"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`
}

// Publisher announces that a transfer was created, for consumers that do not
// poll the transfers table (customer notification, reconciliation). The
// worker that sends to the rail does poll it, and does not depend on this.
//
// PHASE 5.1 — this interface exists to demonstrate a bug, not to be used.
// Calling it AFTER the database transaction commits is the dual-write
// problem: the commit and the publish are two separate systems, and no
// ordering of them is safe.
//
//	commit, then publish  → crash between: a transfer no consumer hears of
//	publish, then commit  → crash between: an event for a transfer that
//	                        does not exist
//
// Postgres cannot roll back a Kafka publish and Kafka cannot roll back a
// Postgres commit, so there is no retry, defer or careful ordering that
// closes the gap. 5.2 removes this by writing the event inside the same
// transaction as the transfer.
type Publisher interface {
	Publish(ctx context.Context, event TransferEvent) error
}
