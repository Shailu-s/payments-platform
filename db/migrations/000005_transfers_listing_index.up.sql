-- The listing index must match the listing query.
--
-- List orders by (created_at DESC, id DESC) and pages with a keyset filter on
-- the tuple. A single-column index on created_at can serve that, but Postgres
-- has to sort within each created_at group and re-check the id predicate. A
-- composite index in the query's own order matches exactly.
CREATE INDEX transfers_created_at_id_idx ON transfers (created_at DESC, id DESC);

-- Superseded: every query that used it is served better by the composite above.
DROP INDEX IF EXISTS transfers_created_at_idx;
