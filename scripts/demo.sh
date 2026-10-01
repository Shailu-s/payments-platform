#!/usr/bin/env bash
#
# A guided walk through the transfer API, against a running API: auth, the
# ledger, idempotency, overspend, the outbox, listing and rate limiting.
# Every step prints what it is proving before it runs.
#
#   make up && make migrate-up      # once
#   make run                        # in another terminal
#   ./scripts/demo.sh
#
set -euo pipefail

API="${API:-http://localhost:8080}"
DB_URL="${DB_URL:-postgres://payments:payments@localhost:5433/payments?sslmode=disable}"
COMPOSE="docker compose -f docker/docker-compose.yml"

bold=$(tput bold 2>/dev/null || true); dim=$(tput dim 2>/dev/null || true)
green=$(tput setaf 2 2>/dev/null || true); red=$(tput setaf 1 2>/dev/null || true)
reset=$(tput sgr0 2>/dev/null || true)

step() { printf "\n${bold}%s${reset}\n" "$1"; }
why()  { printf "${dim}  %s${reset}\n" "$1"; }
pass() { printf "  ${green}PASS${reset}  %s\n" "$1"; }
fail() { printf "  ${red}FAIL${reset}  %s\n" "$1"; FAILURES=$((FAILURES+1)); }

FAILURES=0

# expect <description> <actual> <wanted>
expect() {
  if [ "$2" = "$3" ]; then pass "$1 → $2"; else fail "$1 → got $2, want $3"; fi
}

psql_q() { $COMPOSE exec -T postgres psql -U payments -d payments -tAc "$1" | tr -d '[:space:]'; }
json()   { python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }
newkey() { python3 -c "import uuid;print(uuid.uuid4())"; }

# Every POST /v1/transfers needs an Idempotency-Key (phase 3). Without one the
# API answers 400 before looking at the body, so a check that expects a 400 for
# some other reason would pass for the wrong one.
#   post_transfer <idempotency-key> <body>       prints the response body
#   post_transfer_code <idempotency-key> <body>  prints only the status code
post_transfer() {
  curl -s -H "Authorization: Bearer $KEY" -H "Idempotency-Key: $1" -X POST "$API/v1/transfers" -d "$2"
}
post_transfer_code() {
  curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $KEY" -H "Idempotency-Key: $1" \
    -X POST "$API/v1/transfers" -d "$2"
}
body() { printf '{"source_account":"%s","destination_account":"%s","amount":%s,"currency":"USD"}' "$1" "$2" "$3"; }
balance() { curl -s -H "Authorization: Bearer $KEY" "$API/v1/accounts/$1" | json '["balance"]'; }

curl -sf "$API/healthz" >/dev/null 2>&1 || {
  printf "${red}The API is not running at %s${reset}\n" "$API"
  printf "Start it with:  make up && make migrate-up && make run\n"
  exit 1
}

printf "${bold}Payments platform — walkthrough${reset}\n"
printf "${dim}API %s${reset}\n" "$API"

# ─────────────────────────────────────────────────────────────────────────
step "0. A clean slate"
why "Wiping every table, then restoring the settlement account that migration 000003 creates."
# The table list comes from the catalogue, not by hand. A hand-written list went
# stale when the outbox arrived: it wiped transfers but kept their outbox
# events, and the relay then published an event for a transfer that no longer
# existed. schema_migrations is kept, or migrate would think nothing is applied.
$COMPOSE exec -T postgres psql -U payments -d payments >/dev/null <<'SQL'
DO $$
BEGIN
  EXECUTE (
    SELECT 'TRUNCATE ' || string_agg(format('%I', tablename), ', ') || ' CASCADE'
    FROM pg_tables
    WHERE schemaname = 'public' AND tablename <> 'schema_migrations');
END $$;
INSERT INTO accounts (id, currency, type) VALUES ('acc_settlement_usd', 'USD', 'settlement');
SQL
pass "database reset"

# ─────────────────────────────────────────────────────────────────────────
step "1. Authentication — a key is stored as a hash, never as itself"
why "A stolen database dump must not yield working keys."

KEY_OUTPUT=$(DATABASE_URL="$DB_URL" go run ./cmd/apikey -name "demo key" 2>/dev/null)
KEY=$(echo "$KEY_OUTPUT" | awk '/^  key /{print $2}')
KEY_ID=$(echo "$KEY_OUTPUT" | awk '/^  id /{print $2}')
printf "  minted   %s\n" "$KEY"

STORED=$(psql_q "SELECT key_hash FROM api_keys WHERE id = '$KEY_ID'")
printf "  stored   %s\n" "$STORED"
if [ "$STORED" = "$KEY" ]; then fail "the plaintext key is in the database"
else pass "the database holds a SHA-256 hash, not the key"; fi

SECRET="${KEY#pk_live_}"
expect "no row contains the secret itself" \
  "$(psql_q "SELECT count(*) FROM api_keys WHERE key_hash LIKE '%$SECRET%'")" "0"
pass "the prefix identifies the key without being usable as one: $(psql_q "SELECT prefix FROM api_keys WHERE id='$KEY_ID'")"

# ─────────────────────────────────────────────────────────────────────────
step "2. No key, no money"
why "An unauthenticated POST /transfers moves money for anyone who can reach the port."

CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API/v1/accounts" -d '{"currency":"USD","type":"asset"}')
expect "POST /v1/accounts with no key" "$CODE" "401"

CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer pk_live_notarealkeyatallnotarealkeyatallnotareal" "$API/v1/transfers")
expect "a key that was never issued" "$CODE" "401"

# ─────────────────────────────────────────────────────────────────────────
step "3. Revocation withdraws access but keeps the audit trail"
why "Transfers reference the key that authorised them, so revocation is a timestamp, not a DELETE."

DOOMED=$(DATABASE_URL="$DB_URL" go run ./cmd/apikey -name "to be revoked" 2>/dev/null)
DOOMED_KEY=$(echo "$DOOMED" | awk '/^  key /{print $2}')
DOOMED_ID=$(echo "$DOOMED" | awk '/^  id /{print $2}')

CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $DOOMED_KEY" "$API/v1/transfers")
expect "before revocation" "$CODE" "200"

DATABASE_URL="$DB_URL" go run ./cmd/apikey -revoke "$DOOMED_ID" >/dev/null 2>&1
CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $DOOMED_KEY" "$API/v1/transfers")
expect "after revocation" "$CODE" "401"
expect "the row survives revocation" "$(psql_q "SELECT count(*) FROM api_keys WHERE id = '$DOOMED_ID'")" "1"

# ─────────────────────────────────────────────────────────────────────────
step "4. Accounts, and a balance that is derived rather than stored"
why "There is no balance column. A stored number can disagree with the entries; a derived one cannot."

SRC=$(curl -s -H "Authorization: Bearer $KEY" -X POST "$API/v1/accounts" \
  -d '{"currency":"USD","type":"asset"}' | json '["id"]')
DST=$(curl -s -H "Authorization: Bearer $KEY" -X POST "$API/v1/accounts" \
  -d '{"currency":"USD","type":"liability"}' | json '["id"]')
printf "  source       %s\n" "$SRC"
printf "  destination  %s\n" "$DST"

expect "no balance column exists" \
  "$(psql_q "SELECT count(*) FROM information_schema.columns WHERE table_name='accounts' AND column_name='balance'")" "0"

CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $KEY" "$API/v1/accounts/acc_never_created")
expect "an account that does not exist is 404, not a zero balance" "$CODE" "404"

why "There is no deposit endpoint in V1, so the source is funded with \$10,000 by a balanced"
why "ledger transaction written directly, from a demo funding account — as the tests do."
$COMPOSE exec -T postgres psql -U payments -d payments >/dev/null <<SQL
INSERT INTO accounts (id, currency, type) VALUES ('acc_demo_funding', 'USD', 'liability');
INSERT INTO ledger_transactions (id, reference) VALUES ('ltx_demo_funding', 'demo funding');
INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount) VALUES
  ('ent_demo_funding_dr', 'ltx_demo_funding', 'acc_demo_funding', 'debit',  1000000),
  ('ent_demo_funding_cr', 'ltx_demo_funding', '$SRC',             'credit', 1000000);
SQL
expect "source funded" "$(balance "$SRC")" "1000000"

# ─────────────────────────────────────────────────────────────────────────
step "5. A transfer — 202 processing, and the money is NOT at the destination"
why "We accepted the instruction. The money has not moved. Saying otherwise would be a lie the ledger makes permanent."

IDEM=$(newkey)
RESPONSE=$(post_transfer "$IDEM" "$(body "$SRC" "$DST" 50000)")
echo "$RESPONSE" | python3 -m json.tool | sed 's/^/  /'
TRANSFER=$(echo "$RESPONSE" | json '["id"]')

expect "status" "$(echo "$RESPONSE" | json '["status"]')" "processing"

printf "\n  ${bold}balances after a \$500.00 transfer${reset}\n"
for account in "$SRC:source" "$DST:destination" "acc_settlement_usd:settlement"; do
  id="${account%%:*}"; label="${account##*:}"
  balance=$(curl -s -H "Authorization: Bearer $KEY" "$API/v1/accounts/$id" | json '["balance"]')
  printf "    %-12s %8s\n" "$label" "$balance"
done
why "The destination is 0 and settlement is 50000: the money is ours, earmarked, until the provider confirms."
why "(With \`make worker\` running, MockBank settles about 2s later and moves it to the destination.)"

expect "source debited" "$(balance "$SRC")" "950000"
expect "destination untouched" "$(balance "$DST")" "0"
expect "settlement credited" "$(balance acc_settlement_usd)" "50000"

# ─────────────────────────────────────────────────────────────────────────
step "6. Guarantee 2 — a retried request is answered, not repeated"
why "A client that timed out cannot tell whether the payment happened, so it retries. The retry must not pay twice."

REPLAY=$(post_transfer "$IDEM" "$(body "$SRC" "$DST" 50000)")
expect "the retry returns the same transfer" "$(echo "$REPLAY" | json '["id"]')" "$TRANSFER"
expect "one transfer exists for that key" \
  "$(psql_q "SELECT count(*) FROM transfers WHERE idempotency_key = '$IDEM'")" "1"
expect "the same key with a different body is refused" \
  "$(post_transfer_code "$IDEM" "$(body "$SRC" "$DST" 99)")" "422"

# ─────────────────────────────────────────────────────────────────────────
step "7. Guarantee 3 — money cannot be overspent"
why "The balance is read under a row lock in the transaction that spends it, so two requests cannot both spend the same money."

BEFORE=$(psql_q "SELECT count(*) FROM transfers")
expect "a transfer larger than the balance" \
  "$(post_transfer_code "$(newkey)" "$(body "$SRC" "$DST" 99999999)")" "422"
expect "and it wrote nothing" "$(psql_q "SELECT count(*) FROM transfers")" "$BEFORE"

# ─────────────────────────────────────────────────────────────────────────
step "8. The transfer and its event commit together — the transactional outbox"
why "Publishing to Kafka from the handler loses the event if the process dies after the commit. The event is a row in the same transaction instead."

expect "exactly one event for the transfer" \
  "$(psql_q "SELECT count(*) FROM outbox_events WHERE aggregate_id = '$TRANSFER'")" "1"
expect "its type" \
  "$(psql_q "SELECT event_type FROM outbox_events WHERE aggregate_id = '$TRANSFER'")" "transfer.created"
expect "the retry in step 6 wrote no second event" \
  "$(psql_q "SELECT count(*) FROM outbox_events")" "1"
why "With \`make relay\` running, published_at is already set and the event is in the transfers topic."

# ─────────────────────────────────────────────────────────────────────────
step "9. Guarantee 1 — the ledger always balances"
why "Every debit has a matching credit, so every entry in the database sums to zero."

expect "sum of every ledger entry" \
  "$(psql_q "SELECT COALESCE(SUM(CASE WHEN direction='credit' THEN amount ELSE -amount END),0) FROM ledger_entries")" "0"

# ─────────────────────────────────────────────────────────────────────────
step "10. Guarantee 5 — every financial change records who asked for it"
why "The transfer carries the api key that authorised it."

expect "the transfer is attributed to the calling key" \
  "$(psql_q "SELECT api_key_id FROM transfers WHERE id = '$TRANSFER'")" "$KEY_ID"

# ─────────────────────────────────────────────────────────────────────────
step "11. Atomicity — a rejected transfer writes nothing at all"
why "A transfer row with no accounting behind it is an instruction nobody recorded."

BEFORE=$(psql_q "SELECT count(*) FROM transfers")
post_transfer "$(newkey)" "$(body acc_does_not_exist "$DST" 999)" >/dev/null
expect "no transfer row was created" "$(psql_q "SELECT count(*) FROM transfers")" "$BEFORE"

# ─────────────────────────────────────────────────────────────────────────
step "12. Money is an integer — a fractional amount is refused, not truncated"
why "\$10.50 is 1050. A float cannot represent most decimal fractions, and the error accumulates into real missing money."

CODE=$(post_transfer_code "$(newkey)" "$(body "$SRC" "$DST" 500.75)")
expect "amount 500.75" "$CODE" "400"

CODE=$(post_transfer_code "$(newkey)" "$(body "$SRC" "$SRC" 100)")
expect "an account paying itself" "$CODE" "400"

# ─────────────────────────────────────────────────────────────────────────
step "13. Listing pages without skipping or repeating"
why "Cursors are keyset, not OFFSET: an offset page skips or repeats rows as new transfers arrive."

for amount in 100 200 300 400; do
  post_transfer "$(newkey)" "$(body "$SRC" "$DST" "$amount")" >/dev/null
done

SEEN=""; CURSOR=""; PAGES=0
while :; do
  URL="$API/v1/transfers?limit=2"
  [ -n "$CURSOR" ] && URL="$URL&cursor=$CURSOR"
  PAGE=$(curl -s -H "Authorization: Bearer $KEY" "$URL")
  IDS=$(echo "$PAGE" | python3 -c 'import sys,json;print(" ".join(t["id"] for t in json.load(sys.stdin)["data"]))')
  PAGES=$((PAGES+1))
  printf "  page %d: %s\n" "$PAGES" "$(echo "$IDS" | tr ' ' '\n' | cut -c1-14 | tr '\n' ' ')"
  SEEN="$SEEN $IDS"
  CURSOR=$(echo "$PAGE" | python3 -c 'import sys,json;print(json.load(sys.stdin)["next_cursor"] or "")')
  [ -z "$CURSOR" ] && break
done

TOTAL=$(echo $SEEN | wc -w | tr -d ' ')
UNIQUE=$(echo $SEEN | tr ' ' '\n' | sort -u | wc -l | tr -d ' ')
expect "every transfer appeared exactly once across $PAGES pages" "$TOTAL" "$UNIQUE"
expect "all 5 transfers were listed" "$UNIQUE" "5"

# ─────────────────────────────────────────────────────────────────────────
step "14. Rate limiting — the limit holds under real concurrency"
why "A read-then-write counter lets every concurrent request read the same stale value. Only the database can serialise it."

LIMIT=$(curl -s -D - -o /dev/null -H "Authorization: Bearer $KEY" "$API/v1/transfers" \
  | tr -d '\r' | grep -i '^x-ratelimit-limit:' | cut -d' ' -f2)
if ! [ "$LIMIT" -gt 0 ] 2>/dev/null; then
  fail "could not read the X-RateLimit-Limit header"
  LIMIT=100
fi
printf "  the configured limit is %s requests per minute\n" "$LIMIT"

$COMPOSE exec -T postgres psql -U payments -d payments -c "TRUNCATE rate_limits;" >/dev/null
OVER=$((LIMIT + 20))
WINDOW_AT_START=$(date +%M)
printf "  firing %s requests, 20 at a time…\n" "$OVER"

CODES=$(seq 1 "$OVER" | xargs -P 20 -I{} curl -s -o /dev/null -w "%{http_code}\n" \
  -H "Authorization: Bearer $KEY" "$API/v1/transfers")
ALLOWED=$(echo "$CODES" | grep -c '^200$' || true)
REFUSED=$(echo "$CODES" | grep -c '^429$' || true)

printf "    allowed       %s\n" "$ALLOWED"
printf "    rate limited  %s\n" "$REFUSED"

if [ "$(date +%M)" != "$WINDOW_AT_START" ]; then
  why "The clock minute rolled over mid-run, so the count reset. That is fixed-window"
  why "behaviour, not a bug — rerun to see the limit hold. Skipping this assertion."
else
  expect "exactly the limit was allowed" "$ALLOWED" "$LIMIT"
  expect "every attempt was counted, refused ones included" \
    "$(psql_q "SELECT sum(count) FROM rate_limits")" "$OVER"
fi

printf "\n  ${bold}the refusal${reset}\n"
curl -s -D - -H "Authorization: Bearer $KEY" "$API/v1/transfers" \
  | grep -iE "^HTTP|^retry-after|^x-ratelimit|rate_limited" | sed 's/^/    /'

# ─────────────────────────────────────────────────────────────────────────
step "15. Guarantee 6 — ledger entries are never modified"
why "If something is wrong, a NEW correcting transaction is written. The past is never rewritten."

ENTRY=$(psql_q "SELECT id FROM ledger_entries LIMIT 1")
if $COMPOSE exec -T postgres psql -U payments -d payments -c \
  "UPDATE ledger_entries SET amount = 1 WHERE id = '$ENTRY'" >/dev/null 2>&1; then
  why "NOTE: the database permits UPDATE today. Immutability is a discipline in the code, not yet a constraint."
  $COMPOSE exec -T postgres psql -U payments -d payments -c \
    "UPDATE ledger_entries SET amount = 50000 WHERE id = '$ENTRY'" >/dev/null 2>&1 || true
fi
expect "no UPDATE of ledger_entries exists anywhere in the source" \
  "$(grep -rn 'UPDATE ledger_entries' --include='*.go' --include='*.sql' . | wc -l | tr -d ' ')" "0"

# ─────────────────────────────────────────────────────────────────────────
printf "\n${bold}────────────────────────────────────────${reset}\n"
if [ "$FAILURES" -eq 0 ]; then
  printf "${green}${bold}Everything passed.${reset}\n\n"
else
  printf "${red}${bold}%s check(s) failed.${reset}\n\n" "$FAILURES"
  exit 1
fi
