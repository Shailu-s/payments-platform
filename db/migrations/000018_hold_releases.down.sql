DROP TABLE pending_reversals;
DROP INDEX card_authorizations_open_holds_idx;
ALTER TABLE card_authorizations
    DROP COLUMN expired_at,
    DROP COLUMN released_amount,
    DROP COLUMN held_amount;
