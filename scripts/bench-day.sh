#!/usr/bin/env bash
# One simulated card day against a disposable database, then its books.
# Never touches the application database: the target must be named
# payments_bench*.
#
#   scripts/bench-day.sh
#   TAP_RATE=500 REVERSAL_RATE=50 HOT_RATE=20 DURATION=60s BATCH_AFTER=20 scripts/bench-day.sh
#
# 1. seed 1,000 funded cards and a hot card, start the real API;
# 2. cmd/benchday -prepare: the network simulator plays the day's scenarios
#    (the holds a 10,000-line clearing file clears) and places 2,000 holds old
#    enough to expire;
# 3. k6 drives taps on every card, taps on the hot card and authorizations
#    with their reversals at fixed rates for DURATION; BATCH_AFTER seconds in,
#    cmd/benchday -batch imports the clearing file while the expirer runs;
# 4. cmd/benchday -close settles the day, reconciles it against the network's
#    statement, checks the ledger and reports latency before, during and
#    after the batch jobs. It exits non-zero if any check fails.
set -euo pipefail
cd "$(dirname "$0")/.."

BENCH_DATABASE_URL="${BENCH_DATABASE_URL:-postgres://payments:payments@localhost:5433/payments_bench_day?sslmode=disable}"
ADMIN_URL="${BENCH_ADMIN_URL:-${BENCH_DATABASE_URL%/*}/postgres?sslmode=disable}"
API_ADDR="${BENCH_API_ADDR:-127.0.0.1:18181}"
TAP_RATE="${TAP_RATE:-500}"
REVERSAL_RATE="${REVERSAL_RATE:-50}"
HOT_RATE="${HOT_RATE:-20}"
DURATION="${DURATION:-60s}"
BATCH_AFTER="${BATCH_AFTER:-20}"
LABEL="${LABEL:-day-$(git rev-parse --short HEAD)}"
NETWORK_SECRET="$(openssl rand -hex 32)"
export BENCH_DATABASE_URL NETWORK_SECRET

db_name="${BENCH_DATABASE_URL##*/}"; db_name="${db_name%%\?*}"
case "$db_name" in
  payments_bench*) ;;
  *) echo "refusing: $db_name is not a payments_bench* database" >&2; exit 1 ;;
esac

out="$PWD/bench/out/$LABEL"
mkdir -p "$out/bin"
go build -o "$out/bin/api" ./cmd/api
go build -o "$out/bin/benchseed" ./cmd/benchseed
go build -o "$out/bin/benchday" ./cmd/benchday

psql "$ADMIN_URL" -qAt -c "DROP DATABASE IF EXISTS $db_name WITH (FORCE)" -c "CREATE DATABASE $db_name"
migrate -path db/migrations -database "$BENCH_DATABASE_URL" up >/dev/null
"$out/bin/benchseed" -accounts 1000 -keys 10 -cards -out "$out/fixture.json"

api_pid="" k6_pid=""
cleanup() {
  [ -n "$k6_pid" ] && kill "$k6_pid" 2>/dev/null || true
  [ -n "$api_pid" ] && kill "$api_pid" 2>/dev/null && wait "$api_pid" 2>/dev/null || true
}
trap cleanup EXIT
DATABASE_URL="$BENCH_DATABASE_URL" API_ADDR="$API_ADDR" \
  MOCKBANK_WEBHOOK_SECRET="$(openssl rand -hex 32)" "$out/bin/api" > "$out/api.log" 2>&1 &
api_pid=$!
for _ in $(seq 1 50); do
  curl -fs "http://$API_ADDR/readyz" >/dev/null 2>&1 && break
  sleep 0.2
done
curl -fs "http://$API_ADDR/readyz" >/dev/null || { echo "api did not become ready; see $out/api.log" >&2; exit 1; }

"$out/bin/benchday" -prepare -dir "$out" -api "http://$API_ADDR" -fixture "$out/fixture.json"

TAP_RATE="$TAP_RATE" REVERSAL_RATE="$REVERSAL_RATE" HOT_RATE="$HOT_RATE" DURATION="$DURATION" \
  RUN_ID="$LABEL-$(date +%s)" FIXTURE="$out/fixture.json" API_URL="http://$API_ADDR" \
  K6_CSV_TIME_FORMAT=unix_micro \
  k6 run --quiet --no-color --out "csv=$out/k6.csv" bench/k6/day.js > "$out/k6.log" 2>&1 &
k6_pid=$!
sleep "$BATCH_AFTER"
"$out/bin/benchday" -batch -dir "$out"
wait "$k6_pid"
k6_pid=""

"$out/bin/benchday" -close -dir "$out" -k6 "$out/k6.csv" | tee "$out/report.txt"
