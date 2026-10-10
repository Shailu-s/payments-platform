# payments-platform

Payment infrastructure: a backend platform for moving money reliably through external
payment providers. It maintains its own double-entry ledger and stays correct under retries,
concurrency, duplicate events, provider failures, and reconciliation mismatches.

**Status: phases 1–6 merged. Phase 7 (reconciliation) is implemented on
`phase-7-reconciliation`, pending review and merge.** See [PLAN.md](PLAN.md).

---

## The problem

External payment providers are slow, unreliable and inconsistent. They time out after
already succeeding. They send the same notification twice. They report a payment settled
before they reported it started. Sometimes they simply disagree with you about what happened.

This platform sits between an application and those providers: one API to move money, and a
permanent, auditable record of every cent that survives everything above.

## The guarantees

Each of these will be backed by a named automated test.

1. The ledger always balances — every transaction's debits equal its credits.
2. The same idempotency key never creates two transfers.
3. Money cannot be overspent by concurrent requests.
4. Duplicate provider events never duplicate financial effects.
5. Every financial state change has an audit trail.
6. Ledger entries are never modified or deleted.
7. Kafka redelivery never causes duplicate processing.
8. Provider discrepancies are always detected by reconciliation.

## Stack

Go, PostgreSQL, Kafka, Docker Compose. Modular monolith plus workers.

## Running it locally

Needs Docker, Go 1.26, and the `migrate` CLI
(`go install -tags postgres github.com/golang-migrate/migrate/v4/cmd/migrate@latest`).

```sh
cp .env.example .env   # once: every setting lives here; nothing has a fallback
make up            # Postgres :5433, MockBank :8081, Kafka :9092, and the Kafka topics
make migrate-up    # apply db/migrations
make test          # the full suite, against the real Postgres
```

Postgres is on **5433**, not 5432, so it does not collide with a local install.

Every command reads its settings from the environment and **stops at startup if one is
missing** — there are no built-in defaults to silently fall back to. The Makefile loads `.env`
and passes it on, so run commands through `make`. `.env` is git-ignored; `.env.example` is the
committed template.

### Running the system end to end

Five terminals:

```sh
make apikey NAME="local dev"   # once: prints a key, shown only this one time
make run                       # terminal 1 — the API on :8080
make relay                     # terminal 2 — publishes outbox events to Kafka
make sender                    # terminal 3 — sends each transfer to MockBank as its event arrives
make worker                    # terminal 4 — the safety net: sends what the sender missed, after 30s
make demo                      # terminal 5 — walks through the API
```

Two paths can send a transfer, on purpose. The **sender** is the main path and reacts to the
event within a second. The **worker** polls the database and only takes transfers older than
30 seconds, so if Kafka or the relay is down, payments slow down instead of stopping. Both
claim a transfer with the same guarded `UPDATE`, so it is sent once either way.

MockBank already runs in Compose. `make mock-bank` runs it from source instead, on the same
port, so stop the container first (`docker stop payments-mock-bank`).

### Kafka

`make up` starts a single Kafka broker, and a one-shot `kafka-init` container creates the
`transfers` topic with **3 partitions** (it skips the topic if it already exists). Kafka has
no volume, so `make down` deletes every topic and message, and the next `make up` recreates
the topic empty.

Check the topic exists with 3 partitions:

```sh
docker exec payments-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic transfers
```

List every topic:

```sh
docker exec payments-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Read every message on `transfers` from the start, showing key, partition and headers (Ctrl-C to stop):

```sh
docker exec -it payments-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transfers --from-beginning \
  --property print.key=true --property print.partition=true --property print.headers=true
```

List consumer groups, then see how far one group has read on each partition:

```sh
docker exec payments-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list
docker exec payments-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group <group-name>
```

If `kafka-init` failed, its output says why:

```sh
docker logs $(docker ps -aqf name=kafka-init)
```

## Reconciliation

Apply migration 12 with `make migrate-up`. MockBank must be running the new source/image
with `GET /settlements`; rebuilding an existing MockBank container discards its in-memory
history, so do not use a restart to make an old report appear clean.

```sh
make reconcile
make reconcile ARGS='-report bank.csv -run-id run_001'
make reconcile ARGS='-report bank.csv -run-id run_001'
make reconcile ARGS='-list'
make reconcile ARGS='-inspect run_001 -classification AMOUNT_MISMATCH'
make reconcile ARGS='-inspect run_001 -client-reference tr_example'
make reconcile ARGS='-interval 24h'
```

The command reads `DATABASE_URL` and, for downloads, `PROVIDER_URL`. A recurring process
must be supervised by the operator; it stops on SIGINT/SIGTERM and logs failed attempts.
It runs immediately, then waits the interval after each attempt. It is not a durable
scheduler and its next deadline is not retained across restart.

Authenticated read-only routes (same API-key/rate-limit middleware as transfers):

```text
GET /v1/reconciliation/runs?limit=25&offset=0
GET /v1/reconciliation/runs/{id}
GET /v1/reconciliation/runs/{id}/findings?classification=AMOUNT_MISMATCH&client_reference=tr_example
```

Each run atomically retains the raw provider CSV, its SHA-256, a consistent read-only
Repeatable Read internal snapshot, both capture times, six finding counts, and the full
original evidence. Results are append-only at the database level. The same run ID and exact
report bytes return the original run even if transfers subsequently change; different bytes
with that ID are rejected. A new ID deliberately captures a new internal snapshot.
Malformed/oversized reports or failed commits never leave a successful partial run.

Classification rules and internal cutoff/scope are in [the provider contract](docs/mockbank-api.md#reconciliation-snapshot).
The provider and internal captures are not atomic across systems: intervening acceptance or
status changes can create transient exceptions. Old reports are compared with current internal
state, not reconstructed historical state. Inspect the capture times and rerun with a fresh
report; never automatically settle, refund, or resubmit because of a reconciliation finding.
MockBank's report is a complete current in-memory snapshot, not a historical daily statement.

Proofs: `TestProviderDiscrepanciesAreDetectedWithoutFinancialMutation` covers all six outcomes
with persisted original evidence and unchanged transfer/ledger data;
`TestRealBankReportDetectsLostCallbackWithoutSettlingPayment` exercises the real MockBank HTTP
export; `TestConcurrentRerunsPersistOneOriginalSnapshot` covers 20 simultaneous retries;
`TestFailedCommitLeavesNoPartialRunAndCanRetry` injects a final-commit failure.

### Measured reconciliation cost

2026-10-10, Apple M4, macOS/arm64, Go 1.26, PostgreSQL in local Docker. Three samples;
10,000 generated USD payments in each run. Reproduce with `make bench-reconciliation`
(Postgres required; these benchmarks use fresh disposable schemas).

| Work | Time per 10,000 rows | Allocated memory per operation |
| --- | --- | --- |
| CSV parse + validation | 1.77–1.86 ms | 5.60 MB |
| In-memory comparison | 4.83–5.03 ms | 10.94 MB |
| Parse + DB snapshot + comparison + atomic evidence persistence + result read | 164–270 ms (median 189 ms) | 38.18 MB |

The full run added about **5.54 MB of database storage** (including indexes/TOAST) per 10,000
matched payments. Pure benchmarks run for one second per sample; database benchmarks use
three runs per sample. This small local experiment is not a capacity estimate, p99 latency,
HTTP/provider throughput, or a claim that 10,000 payments can settle in 189 ms. Fixture setup
and provider downloading are excluded from the database benchmark. CLI `elapsed_ms` includes
the provider download and persisted run; stored `preparation_ms` excludes result persistence.

The cost is retaining whole snapshots and a finding per payment on every run; storage grows
with both payment history and run count. There is no automatic evidence-retention policy in V1.

### Existing hot-path experiments rerun

On the same machine/date, three runs of `TestLockStrategyComparison` each spent 200
fully affordable payments from one hot account. At 50 writers, `FOR UPDATE` carried all
200 in every sample at 700–1,340 spends/s; sample p99s were 40.4–128.5 ms. The bounded
`SERIALIZABLE` path carried 137–144, with 56–63 failed attempts, at 228–355 spends/s.
This is a database ledger-debit experiment, not full HTTP-to-provider settlement throughput.
The contention shape is deliberately one account, not uniform traffic across accounts.

The Postgres rate limiter measured 0.171–0.253 ms per allowed request, against
0.101–0.144 ms for `SELECT 1`; these were sequential samples, not paired latency measurements.
Both include the local Docker database round trip. Reproduce all three experiments with
`make bench`. The existing hot-path fixtures are reset in package-specific `test_*` schemas;
use `TEST_DATABASE_URL` pointing to a disposable database, never production.

## Scope

V1 is one currency (USD) and one simulated bank rail. Deliberately out: cards, crypto, FX,
real bank integration, Kubernetes, fraud, KYC, microservices. The scope fence and the
reasoning are in [PLAN.md](PLAN.md).
