package worker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Shailu-s/payments-platform/internal/provider"
	"github.com/Shailu-s/payments-platform/internal/transfers"
)

// settleFromLookup records the reference the lookup gave us, then settles. A
// transfer parked as unresolved may never have had a provider_ref stored.
func (w *Worker) settleFromLookup(ctx context.Context, t transfers.Transfer, p provider.Payment) error {
	const storeRef = `
		UPDATE transfers SET provider_ref = COALESCE(provider_ref, $1), updated_at = now()
		WHERE id = $2`
	if _, err := w.db.Exec(ctx, storeRef, p.ProviderRef, t.ID); err != nil {
		return fmt.Errorf("store provider ref for %s: %w", t.ID, err)
	}

	slog.InfoContext(ctx, "unresolved transfer settled by lookup",
		"transfer_id", t.ID, "provider_ref", p.ProviderRef)
	return transfers.Settle(ctx, w.db, t.ID)
}
