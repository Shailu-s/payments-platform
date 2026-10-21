#!/usr/bin/env bash
# HTTP load benchmark for POST /v1/transfers and POST /network/authorizations
# against a disposable database. Never touches the application database: the
# target must be named payments_bench*.
#
#   MODES="spread hot" RATES="100 250 500 1000" DURATION=30s scripts/bench-http.sh
#   MODES="auth-spread auth-hot" RATES="250 500 1000" scripts/bench-http.sh
#
# auth-spread taps a random card among one per source account; auth-hot taps the
# one card on the hot account. Each auth row also reports how many were declined.
# MODE mixed drives the hot account at each RATE while spread traffic runs at
# SPREAD_RATE alongside it, and reports the two as mixed-hot and mixed-spread.
# HOT_HISTORY gives the hot account that many pre-existing entries.
# Each mode first runs WARMUP at its first rate and discards it: the first
# measured step of a fresh API process otherwise carries the worst tail.
# RATE_LIMIT_BACKEND (redis or postgres) picks the API's rate limiter; with
# redis it counts in BENCH_REDIS_URL, a database other than 0 that every
# mode flushes. Builds without the setting ignore it and report null.
set -euo pipefail
cd "$(dirname "$0")/.."

BENCH_DATABASE_URL="${BENCH_DATABASE_URL:-postgres://payments:payments@localhost:5433/payments_bench?sslmode=disable}"
ADMIN_URL="${BENCH_ADMIN_URL:-${BENCH_DATABASE_URL%/*}/postgres?sslmode=disable}"
API_ADDR="${BENCH_API_ADDR:-127.0.0.1:18180}"
MODES="${MODES:-spread hot}"
RATES="${RATES:-100 250 500 1000 1500}"
DURATION="${DURATION:-30s}"
WARMUP="${WARMUP:-15s}"
ACCOUNTS="${ACCOUNTS:-1000}"
KEYS="${KEYS:-3000}"
SPREAD_RATE="${SPREAD_RATE:-300}"
HOT_HISTORY="${HOT_HISTORY:-0}"
RATE_LIMIT_BACKEND="${RATE_LIMIT_BACKEND:-redis}"
BENCH_REDIS_URL="${BENCH_REDIS_URL:-redis://localhost:6379/15}"
LABEL="${LABEL:-$(git rev-parse --short HEAD)}"
# Throwaway, shared by the API and k6 only for this run.
NETWORK_SECRET="$(openssl rand -hex 32)"

db_name="${BENCH_DATABASE_URL##*/}"; db_name="${db_name%%\?*}"
case "$db_name" in
  payments_bench*) ;;
  *) echo "refusing: $db_name is not a payments_bench* database" >&2; exit 1 ;;
esac

# redis://host:port/N without credentials; N must not be 0 (the application's).
if [[ ! "$BENCH_REDIS_URL" =~ ^redis://([^:/@]+):([0-9]+)/([0-9]+)$ ]]; then
  echo "refusing: BENCH_REDIS_URL must look like redis://host:port/N" >&2; exit 1
fi
redis_host="${BASH_REMATCH[1]}" redis_port="${BASH_REMATCH[2]}" redis_db="${BASH_REMATCH[3]}"
if [ "$redis_db" -eq 0 ]; then
  echo "refusing: BENCH_REDIS_URL selects database 0" >&2; exit 1
fi

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
    -accounts "$ACCOUNTS" -keys "$KEYS" -history "$HOT_HISTORY" -cards -out "$out/fixture.json"
  flush_redis
}

# flush_redis empties the benchmark's Redis database (only that index), so no
# mode inherits another's counters. Plain RESP over bash's /dev/tcp: no
# redis-cli needed on the host.
flush_redis() {
  [ "$RATE_LIMIT_BACKEND" = redis ] || return 0
  local reply
  exec 3<>"/dev/tcp/$redis_host/$redis_port"
  printf 'SELECT %s\r\nFLUSHDB\r\n' "$redis_db" >&3
  reply=$(head -c 10 <&3)
  exec 3<&-
  # $(...) drops the final newline.
  if [ "$reply" != $'+OK\r\n+OK\r' ]; then
    echo "redis FLUSHDB on database $redis_db failed: $reply" >&2; exit 1
  fi
}

# run_k6 ROW_MODE MODE RATE KEY_OFFSET STEP [DURATION] writes one row to
# $out/step-ROW_MODE.json. STEP keeps idempotency keys unique when a fixed-rate
# stream runs in every step.
run_k6() {
  rm -f "$out/step-$1.json"
  local script=transfers.js
  case "$2" in auth-*) script=authorizations.js ;; esac
  MODE="$2" ROW_MODE="$1" RATE="$3" KEY_OFFSET="$4" DURATION="${6:-$DURATION}" RUN_ID="$LABEL-$1-$5" \
    FIXTURE="$out/fixture.json" API_URL="http://$API_ADDR" OUT="$out/step-$1.json" \
    NETWORK_SECRET="$NETWORK_SECRET" \
    k6 run --quiet --no-color "bench/k6/$script" > "$out/k6-$1-$5.log" 2>&1
}

# metric NAME prints the value of an unlabelled metric in $metrics, or null.
metric() { awk -v n="$1" '$1 == n { v = $2 } END { print (v == "" ? "null" : v) }' <<<"$metrics"; }

api_pid=""
start_api() {
  DATABASE_URL="$BENCH_DATABASE_URL" API_ADDR="$API_ADDR" \
    MOCKBANK_WEBHOOK_SECRET="$(openssl rand -hex 32)" NETWORK_SECRET="$NETWORK_SECRET" \
    RATE_LIMIT_BACKEND="$RATE_LIMIT_BACKEND" REDIS_URL="$BENCH_REDIS_URL" \
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
  first_rate="${RATES%% *}"
  for rate in warmup $RATES; do
    step="$rate" step_duration="$DURATION"
    if [ "$rate" = warmup ]; then
      rate="$first_rate" step_duration="$WARMUP"
    fi
    if [ "$mode" = mixed ]; then
      rows="mixed-hot mixed-spread"
      run_k6 mixed-hot hot "$rate" 0 "$step" "$step_duration" &
      hot_pid=$!
      run_k6 mixed-spread spread "$SPREAD_RATE" $((KEYS / 2)) "$step" "$step_duration"
      wait "$hot_pid"
    else
      rows="$mode"
      run_k6 "$mode" "$mode" "$rate" 0 "$step" "$step_duration"
    fi
    if [ "$step" = warmup ]; then
      for row in $rows; do
        echo "warm-up discarded: $(cat "$out/step-$row.json")"
      done
      sleep 2
      continue
    fi
    entries=$(psql "$BENCH_DATABASE_URL" -qAt -c "SELECT count(*) FROM ledger_entries")
    drift=$(BENCH_DATABASE_URL="$BENCH_DATABASE_URL" "$out/bin/benchseed" -verify)
    # Cumulative over the mode's steps, like ledger_entries_after.
    declined=$(psql "$BENCH_DATABASE_URL" -qAt -c "SELECT count(*) FROM card_authorizations WHERE decision = 'declined'")
    # Cumulative too: time in the rate limiter and in the requests it checked.
    # Absent (null) on builds that do not export them.
    metrics=$(curl -fs -H "Authorization: Bearer $(jq -r '.keys[0]' "$out/fixture.json")" "http://$API_ADDR/metrics" || true)
    rl_checks=$(metric payments_ratelimit_checks_total)
    rl_seconds=$(metric payments_ratelimit_check_seconds_total)
    rl_request_seconds=$(metric payments_ratelimited_request_seconds_total)
    rl_failopen=$(metric payments_ratelimit_failopen_total)
    # The backend the API reported at startup; null on builds that do not.
    rl_backend=$(jq -cR 'fromjson? | select(.msg == "rate limiter") | .backend' "$out/api.log" | head -1 || true)
    for row in $rows; do
      jq -c --argjson e "$entries" --argjson d "$drift" --argjson x "$declined" --arg w "$WARMUP" \
        --argjson rc "$rl_checks" --argjson rs "$rl_seconds" --argjson rr "$rl_request_seconds" \
        --argjson rf "$rl_failopen" --argjson rb "${rl_backend:-null}" \
        '. + {ledger_entries_after: $e, balance_drift: $d, declined_after: $x, warmup: $w,
              ratelimit_checks_after: $rc, ratelimit_seconds_after: $rs, ratelimited_request_seconds_after: $rr,
              ratelimit_backend: $rb, ratelimit_failopen_after: $rf}' \
        "$out/step-$row.json" | tee -a "$results"
    done
    sleep 2
  done
  stop_api
done
echo "results: $results"
