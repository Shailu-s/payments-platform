package cards

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrInvalidState means the card's status does not allow the transition.
var ErrInvalidState = errors.New("card state does not allow this change")

// Action is a status change: the statuses it may start from, where it ends,
// and the event that records it.
type Action struct {
	Name  string
	From  []string
	To    string
	Event string
}

var (
	Freeze   = Action{Name: "freeze", From: []string{StatusActive}, To: StatusFrozen, Event: "card.frozen"}
	Unfreeze = Action{Name: "unfreeze", From: []string{StatusFrozen}, To: StatusActive, Event: "card.unfrozen"}
	Close    = Action{Name: "close", From: []string{StatusActive, StatusFrozen}, To: StatusClosed, Event: "card.closed"}
)

// Transition applies a to the card and returns the card as it now is.
//
// The status guard is in the UPDATE itself, so the row lock serialises
// concurrent changes and a waiting UPDATE re-checks the guard against the
// committed status: a freeze racing a close can never reopen a closed card.
// A card already in the target state is returned unchanged with no event,
// so a retried request is harmless.
func Transition(ctx context.Context, db Beginner, id string, a Action) (Card, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return Card{}, fmt.Errorf("begin %s %s: %w", a.Name, id, err)
	}
	defer tx.Rollback(ctx)

	const q = `
		UPDATE cards c SET status = $2, updated_at = now()
		FROM card_accounts ca
		WHERE c.id = $1 AND c.status = ANY($3) AND ca.account_id = c.account_id
		RETURNING ` + columns

	card, err := scan(tx.QueryRow(ctx, q, id, a.To, a.From))
	if err == nil {
		if err := insertEvent(ctx, tx, a.Event, card); err != nil {
			return Card{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Card{}, fmt.Errorf("commit %s %s: %w", a.Name, id, err)
		}
		return card, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Card{}, fmt.Errorf("%s %s: %w", a.Name, id, err)
	}

	// No row: the card is missing, already there, or in a state a forbids.
	card, err = Get(ctx, tx, id)
	if err != nil {
		return Card{}, err
	}
	if card.Status == a.To {
		return card, nil
	}
	return card, ErrInvalidState
}
