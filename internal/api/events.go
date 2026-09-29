package api

// TransferEvent is what a consumer needs to act on a transfer without
// re-reading everything about it. Kept small on purpose: an event is a
// notification that something happened, not a copy of the row.
//
// It is a separate type from transfers.Transfer deliberately. The row carries
// private bookkeeping (api key, idempotency key, fingerprint, retry state) that
// must not be broadcast, and its Go field names are free to change. This type
// is the contract consumers read, so it changes only on purpose.
type TransferEvent struct {
	TransferID         string `json:"transfer_id"`
	Amount             int64  `json:"amount"`
	Currency           string `json:"currency"`
	SourceAccount      string `json:"source_account"`
	DestinationAccount string `json:"destination_account"`
}
