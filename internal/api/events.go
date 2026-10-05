package api

// TransferEvent is the public consumer contract, excluding private key and retry metadata.
type TransferEvent struct {
	TransferID         string `json:"transfer_id"`
	Amount             int64  `json:"amount"`
	Currency           string `json:"currency"`
	SourceAccount      string `json:"source_account"`
	DestinationAccount string `json:"destination_account"`
}
