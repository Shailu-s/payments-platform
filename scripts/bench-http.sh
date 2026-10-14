#!/usr/bin/env bash
# HTTP load benchmark for POST /v1/transfers against a disposable database.
# Never touches the application database: the target must be named payments_bench*.
#
#   MODES="spread hot" RATES="100 250 500 1000" DURATION=30s scripts/bench-http.sh
#
# MODE mixed drives the hot account at each RATE while spread traffic runs at
# SPREAD_RATE alongside it, and reports the two as mixed-hot and mixed-spread.
# HOT_HISTORY gives the hot account that many pre-existing entries.
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
SPREAD_RATE="${SPREAD_RATE:-300}"
HOT_HISTORY="${HOT_HISTORY:-0}"
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
    -accounts "$ACCOUNTS" -keys "$KEYS" -history "$HOT_HISTORY" -out "$out/fixture.json"
}

# run_k6 ROW_MODE MODE RATE KEY_OFFSET STEP writes one row to $out/step-ROW_MODE.json.
# STEP keeps idempotency keys unique when a fixed-rate stream runs in every step.
run_k6() {
  rm -f "$out/step-$1.json"
  MODE="$2" ROW_MODE="$1" RATE="$3" KEY_OFFSET="$4" DURATION="$DURATION" RUN_ID="$LABEL-$1-$5" \
    FIXTURE="$out/fixture.json" API_URL="http://$API_ADDR" OUT="$out/step-$1.json" \
    k6 run --quiet --no-color "bench/k6/transfers.js" > "$out/k6-$1-$5.log" 2>&1
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
    if [ "$mode" = mixed ]; then
      rows="mixed-hot mixed-spread"
      run_k6 mixed-hot hot "$rate" 0 "$rate" &
      hot_pid=$!
      run_k6 mixed-spread spread "$SPREAD_RATE" $((KEYS / 2)) "$rate"
      wait "$hot_pid"
    else
      rows="$mode"
      run_k6 "$mode" "$mode" "$rate" 0 "$rate"
    fi
    entries=$(psql "$BENCH_DATABASE_URL" -qAt -c "SELECT count(*) FROM ledger_entries")
    drift=$(BENCH_DATABASE_URL="$BENCH_DATABASE_URL" "$out/bin/benchseed" -verify)
    for row in $rows; do
      jq -c --argjson e "$entries" --argjson d "$drift" \
        '. + {ledger_entries_after: $e, balance_drift: $d}' "$out/step-$row.json" | tee -a "$results"
    done
    sleep 2
  done
  stop_api
done
echo "results: $results"
