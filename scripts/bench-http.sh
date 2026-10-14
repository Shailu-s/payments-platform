#!/usr/bin/env bash
# HTTP load benchmark for POST /v1/transfers against a disposable database.
# Never touches the application database: the target must be named payments_bench*.
#
#   MODES="spread hot" RATES="100 250 500 1000" DURATION=30s scripts/bench-http.sh
set -euo pipefail
cd "$(dirname "$0")/.."

BENCH_DATABASE_URL="${BENCH_DATABASE_URL:-postgres://payments:payments@localhost:5433/payments_bench?sslmode=disable}"
ADMIN_URL="${BENCH_ADMIN_URL:-${BENCH_DATABASE_URL%/*}/postgres?sslmode=disable}"
API_ADDR="${BENCH_API_ADDR:-127.0.0.1:18180}"
MODES="${MODES:-spread hot}"
RATES="${RATES:-100 250 500 1000 1500}"
DURATION="${DURATION:-30s}"
ACCOUNTS="${ACCOUNTS:-1000}"
KEYS="${KEYS:-3000}"
LABEL="${LABEL:-$(git rev-parse --short HEAD)}"

db_name="${BENCH_DATABASE_URL##*/}"; db_name="${db_name%%\?*}"
case "$db_name" in
  payments_bench*) ;;
  *) echo "refusing: $db_name is not a payments_bench* database" >&2; exit 1 ;;
esac

out="$PWD/bench/out/$LABEL"
mkdir -p "$out/bin"
results="$out/results.jsonl"
: > "$results"

go build -o "$out/bin/api" ./cmd/api
go build -o "$out/bin/benchseed" ./cmd/benchseed

reset_db() {
  psql "$ADMIN_URL" -qAt -c "DROP DATABASE IF EXISTS $db_name WITH (FORCE)" -c "CREATE DATABASE $db_name"
  migrate -path db/migrations -database "$BENCH_DATABASE_URL" up >/dev/null
  BENCH_DATABASE_URL="$BENCH_DATABASE_URL" "$out/bin/benchseed" \
    -accounts "$ACCOUNTS" -keys "$KEYS" -out "$out/fixture.json"
}

api_pid=""
start_api() {
  DATABASE_URL="$BENCH_DATABASE_URL" API_ADDR="$API_ADDR" \
    MOCKBANK_WEBHOOK_SECRET="$(openssl rand -hex 32)" \
    "$out/bin/api" > "$out/api.log" 2>&1 &
  api_pid=$!
  for _ in $(seq 1 50); do
    curl -fs "http://$API_ADDR/readyz" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "api did not become ready; see $out/api.log" >&2; exit 1
}
stop_api() { [ -n "$api_pid" ] && kill "$api_pid" 2>/dev/null && wait "$api_pid" 2>/dev/null || true; api_pid=""; }
trap stop_api EXIT

for mode in $MODES; do
  # Every mode starts from the same fresh fixture.
  reset_db
  start_api
  for rate in $RATES; do
    rm -f "$out/step.json"
    MODE="$mode" RATE="$rate" DURATION="$DURATION" RUN_ID="$LABEL-$mode-$rate" \
      FIXTURE="$out/fixture.json" API_URL="http://$API_ADDR" OUT="$out/step.json" \
      k6 run --quiet --no-color "bench/k6/transfers.js" > "$out/k6-$mode-$rate.log" 2>&1
    entries=$(psql "$BENCH_DATABASE_URL" -qAt -c "SELECT count(*) FROM ledger_entries")
    jq -c --argjson e "$entries" '. + {ledger_entries_after: $e}' "$out/step.json" | tee -a "$results"
    sleep 2
  done
  stop_api
done
echo "results: $results"
