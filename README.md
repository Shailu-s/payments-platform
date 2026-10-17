# payments-platform

Payment infrastructure for reliably moving money through external providers. A double-entry
ledger stays correct under retries, concurrent spending, duplicate events, provider failures,
and reconciliation mismatches.

**Status:** V1 is implemented in this source snapshot, including the closure guardrails,
minimal metrics and end-to-end failure proof. All seven build phases are merged; [PLAN.md](PLAN.md)
records review/merge state, decisions and the build log. Upgrading a running sandbox still
requires explicit migrations and deployment of the new binaries.

## Eight guarantees, with their proof

| Guarantee | Named automated proof |
| --- | --- |
| Every committed ledger transaction balances. | [TestRecordMovesMoneyAndBalances](internal/ledger/record_test.go), [TestRecordRejectsOverflowingTotals](internal/ledger/overflow_test.go) |
| One transfer (or card) per API key + idempotency key, including concurrent retries. | [TestIdempotentRequestsCreateOneTransfer](internal/api/idempotency_test.go), [TestRetryAfterFundsAreExhaustedReplaysTheOriginal](internal/api/idempotency_test.go), [TestIssueCardIsIdempotent](internal/api/cards_test.go) |
| Concurrent requests cannot overspend an account. | [TestConcurrentTransfersCannotOverspend](internal/api/overspend_test.go), [TestLedgerRefusesOverdraft](internal/ledger/stored_balance_test.go), [TestSecondSpendWaitsForTheFirstAndSeesItsDebit](internal/api/overspend_test.go), [TestStoredBalanceTracksEveryMovement](internal/ledger/stored_balance_test.go) |
| Duplicate provider events have one financial effect. | [TestConcurrentDuplicateWebhooksHaveOneEffect](internal/api/webhooks_test.go), [TestWebhookRetriesAfterFailedTransaction](internal/api/webhooks_test.go) |
| Every financial transfer transition and every card change retains audit evidence. | [TestEveryFinancialTransitionRetainsAuditEvidence](internal/api/audit_test.go), [TestTerminalOutboxFailureRollsBackFinancialEffect](internal/api/terminal_events_test.go), [TestEveryCardChangeIsAuditedInItsTransaction](internal/api/cards_transitions_test.go) |
| Ledger history rejects UPDATE/DELETE; corrections append new entries. | [TestLedgerHistoryRejectsUpdatesAndDeletes](internal/ledger/immutable_test.go) |
| Kafka redelivery does not duplicate payment effects or durable notification jobs. | [TestSenderSIGKILLRedeliversWithoutDuplicatePayment](internal/consumer/crash_test.go), [TestKafkaRedeliveryCreatesOneDurableJob](internal/customerwebhooks/worker_test.go) |
| Reconciliation detects all six defined outcomes in validated complete snapshots. | [TestProviderDiscrepanciesAreDetectedWithoutFinancialMutation](internal/reconciliation/evidence_test.go) |

These guarantees cover the implemented API/worker paths and the provider contract, not
arbitrary administrator changes to the database. The ledger writer validates positive integer
minor units, bounded totals, and equal debits/credits before writing. Migration 13 rejects
ordinary UPDATE/DELETE on entries and transaction metadata; trusted administrators can still
TRUNCATE, disable triggers, or issue DDL. No restricted production database role is provisioned
in this local sandbox.

## The problem and the flow

A bank can accept a payment and then time out, deliver a callback twice, or disagree with our
records. The API must not guess that a timeout means failure, refund money blindly, or turn a
retry into a second payment.

```text
POST /v1/transfers + API key + Idempotency-Key
  -> Postgres transaction: intent + source-to-settlement ledger + outbox
  -> relay -> Kafka -> sender -> MockBank
  -> signed provider callback -> destination credit OR source refund + completion outbox
  -> relay -> Kafka -> durable customer delivery -> signed HTTP notification/retries
  -> provider CSV + internal snapshot -> reconciliation findings (detect, never auto-fix)
```

Go 1.26, PostgreSQL, Kafka, Docker Compose. Modular monolith plus workers, not microservices.
Amounts are integer cents: `500` is $5, not a floating-point value. A transfer returns
`processing`; it is not settled until a callback or resolving lookup confirms its outcome.

## Running locally

Needs Docker, Go 1.26, and the golang-migrate v4 CLI built with its Postgres driver.
Only copy the template if `.env` does not already exist:

```sh
test -f .env || cp .env.example .env
```

Set `DATABASE_URL` for the local database; configure distinct random signing keys of at least
32 bytes in `MOCKBANK_WEBHOOK_SECRET` and `CUSTOMER_WEBHOOK_SECRET`, plus a controlled
`CUSTOMER_WEBHOOK_URL`. Keys belong only in the ignored `.env`; never commit or log them.
Every runtime command reads required environment settings and fails rather than silently
choosing another endpoint. Make loads `.env`; it does not export those values into your
interactive shell. Tests have documented local infrastructure defaults, not runtime defaults.

For a **fresh sandbox**:

```sh
make up
make migrate-up
make test
make demo-v1
```

Postgres uses host port **5433**, MockBank **8081**, Kafka **9092**. `make migrate-up` applies
all migrations, including reconciliation (12), ledger immutability (13), stored balances (14),
the deferred transfer-to-ledger link (15) and cards (16), without resetting application tables.
Compose's `kafka-init` creates the `transfers` and `cards` topics.
Migration 14 backfills each non-settlement account's balance from its entries and **fails if
any of them is already negative** (an overdrawn funding account, for example): fix or
re-type that account deliberately; the migration does not clamp. The API's connection pool
size is the DSN parameter `pool_max_conns` (pgxpool; default `max(4, CPUs)`), for example
`...?sslmode=disable&pool_max_conns=20`. When upgrading an existing sandbox, preserve its database and capture
provider evidence before replacing MockBank: its payment history and deduplication state are
in memory and disappear on restart. Running tests does not upgrade your live application schema.

### Isolated, reproducible V1 demonstration

`make demo-v1` runs
[TestV1PaymentSurvivesTimeoutAfterSuccess](cmd/mock-bank/v1_demo_test.go). It needs the local
Postgres and Kafka broker but **does not need or reset the running API/MockBank**, call a real
bank/customer, or touch application tables. It creates disposable random schemas, loopback
HTTP servers, and unique Kafka topics/groups. Topics are retained in the sandbox broker for
inspection; outbox routing is changed only inside its disposable fixtures.

Both normal and timeout-after-success cases exercise real production components:

```text
$10 source fixture -> authenticated $5 transfer -> outbox -> Kafka -> sender -> MockBank
Normal:                 bank acknowledgement -> PROCESSING
Timeout after success:  bank accepts/settles, acknowledgement withheld -> UNRESOLVED
Before callback:        one submission; source $5, destination $0; STATUS_MISMATCH detected
Duplicate signed callbacks -> one settlement; source $5, destination $5
Customer returns 503, then 200 -> same signed event, two attempts, delivered
Ledger -> three balanced transactions / six entries (funding, reservation, settlement)
Final reconciliation -> MATCHED; no automatic repair or second payment
```

Failure timing is controlled by test-only acknowledgement/callback gates, not a shipped Chaos
API. Customer backoff is shortened in this demonstration; the durable retry tests separately
prove the production 1/2/4/8/16-second schedule. This is a correctness proof, not a performance
benchmark or a claim about real bank behaviour.

The older `make demo` script **resets the configured application database**. It is not the V1
closure demo; do not run it against retained data.

### Persistent development processes

Mint an API key once with `make apikey NAME="local dev"` (it is shown once). `make apikeys`
lists prefixes, never secrets. Run these in separate terminals; MockBank is already in Compose:

```sh
make run
make relay
make sender
make worker
make webhook-sender
make reconcile ARGS='-interval 24h'
```

`run` serves the API; `relay` publishes outbox rows; `sender` submits created transfers;
`worker` is the polling safety net and resolves unknown outcomes; `webhook-sender` durably
queues and retries customer notifications; `reconcile` runs immediately, then waits 24 hours
between completed attempts. The sender has a 30-second head start over the poller. Both use
the same guarded claim, so the two paths do not independently pay the same transfer.

`make mock-bank` runs the provider from source using `MOCKBANK_ADDR`. Use a separate free port
and matching provider/callback settings for a second sandbox; do not stop/reset an existing
bank merely to resolve a disagreement. API readiness is `/readyz`, liveness is `/healthz`.

### Verification

```sh
make test
go build ./...
go vet ./...
go test -race ./... -count=1
make chaos-kill-consumer
make bench
```

Tests and existing hot-path benchmarks recreate package-specific `test_*` schemas and reset
fixtures. Use `TEST_DATABASE_URL` pointing to a disposable local database, never production.
The full suite also exercises local Kafka using isolated topics. The crash proof SIGKILLs only
its own test child, not a live worker. `make chaos-kill-consumer` runs that proof independently.

## Minimal metrics

Authenticated `GET /metrics` returns Prometheus text format without a new dependency or
monitoring container. A valid bearer API key is required; scrapes do not spend that key's
payment rate-limit quota. Authentication plus collection share a two-second deadline.

| Metric | Meaning |
| --- | --- |
| `payments_http_requests_total{status_class}` | Process-local completed HTTP request counter; fixed 1xx–5xx labels, excluding metrics scrapes. Resets on API restart. |
| `payments_transfers{status}` | Current database transfer counts across the five defined states. |
| `payments_outbox_pending` | Current unpublished outbox rows. |
| `payments_customer_deliveries{status}` | Current pending/delivered/dead notification counts. |
| `payments_reconciliation_exceptions{classification}` | Exception counts from the latest completed run, not a sum of historical runs. |
| `payments_reconciliation_last_run_timestamp_seconds` | Latest run creation time in Unix seconds; zero means no completed run. |

Database metrics are **gauges**, not process counters. They reflect shared database state:
do not sum the same global gauges across API replicas. No account/transfer/event IDs, URLs,
API keys, arbitrary paths, or request IDs become labels. A failed scrape returns an error,
not partial or zero-valued apparently healthy output. Fixed labels produce 20 series per API
instance; there is no dashboard, tracing, or alert engine in V1.

Proofs: [TestMetricsRequiresAuthAndReportsBoundedHTTPCounts](internal/api/metrics_test.go),
[TestMetricsReflectsDatabaseBacklogsAndLatestReconciliation](internal/api/metrics_test.go),
[TestMetricsScrapesDoNotConsumePaymentQuota](internal/api/metrics_test.go), and the failed/
blocked-database and concurrent-counter tests in the same file.

**Measured cost:** Apple M4/macOS arm64, Go 1.26.0, local Docker Postgres, three samples.
With 1,000 transfers, 1,000 outbox rows, and 1,000 delivery rows, an authenticated in-process
HTTP scrape took **0.530–1.045 ms**, about **19 KB allocated / 172 allocations**. Production
JSON logging was directed to a discard sink; socket/TLS latency and fixture setup are excluded.
The isolated observation microbenchmark measured **16.9–23.5 ns**, **32 B / one allocation**,
against a ~1.38 ns no-op handler. This single-thread microbenchmark is middleware-only cost,
not whole-request latency or concurrent-counter throughput.
Reproduce with `make bench-metrics`.

The cost is an indexed auth lookup plus one aggregate query per scrape; exact transfer and
delivery counts scan retained history. Keep scrape frequency modest at V1 volume. Fixed
labels prevent cardinality growth but do not make database collection constant-time.

## Reconciliation

MockBank must expose `GET /settlements`. Its
[CSV contract](docs/mockbank-api.md#reconciliation-snapshot) defines complete snapshots,
matching identities, input rejection, classification precedence, and internal cutoff/scope.

```sh
make reconcile
make reconcile ARGS='-report bank.csv -run-id run_001'
make reconcile ARGS='-report bank.csv -run-id run_001'
make reconcile ARGS='-list'
make reconcile ARGS='-inspect run_001 -classification AMOUNT_MISMATCH'
make reconcile ARGS='-inspect run_001 -client-reference tr_example'
```

The repeated saved-file invocation is an identical retry: the same ID and exact report bytes
return the original run even if transfers subsequently change. Different bytes cannot reuse
that ID; a new ID deliberately captures fresh internal truth. Each run atomically retains the
raw CSV, its SHA-256, a read-only Repeatable Read snapshot, both capture times, six finding
counts, and original evidence. Malformed/oversized reports and failed commits never leave a
successful partial run. Ordinary UPDATE/DELETE of persisted evidence is rejected.

Authenticated read-only routes:

```text
GET /v1/reconciliation/runs?limit=25&offset=0
GET /v1/reconciliation/runs/{id}
GET /v1/reconciliation/runs/{id}/findings?classification=AMOUNT_MISMATCH&client_reference=tr_example
```

Reconciliation only detects differences. It does not settle, refund, resubmit, or automatically
repair money. The provider and internal captures are not atomic across systems; intervening
acceptance/status changes can produce transient exceptions. Old reports compare against
current state, not reconstructed historical state. Inspect capture times and use a fresh
report when investigating. MockBank exports retained current in-memory history, not a
historical daily settlement statement. Internal failed instructions without a provider
reference are excluded unless the report identifies them by client reference.

The recurring command needs operator supervision: it is process-local, has no durable restart
deadline, and does not automatically retain a missed schedule. Evidence has no automatic
retention/deletion policy. These are deliberate V1 limits, not guarantees of unattended
production operation.

### Recorded reconciliation and hot-path evidence

2026-10-10, Apple M4/macOS arm64, Go 1.26, Postgres in Docker; three samples of 10,000 rows:

| Work | Time per 10,000 rows | Allocated memory |
| --- | --- | --- |
| CSV parse + validation | 1.77–1.86 ms | 5.60 MB |
| In-memory comparison | 4.83–5.03 ms | 10.94 MB |
| Parse + DB snapshot + compare + atomic persistence + result read | 164–270 ms (median 189 ms) | 38.18 MB |

About **5.54 MB additional DB storage**, including indexes/TOAST, per 10,000 matched payments
per run. Pure benchmarks run one second per sample; DB samples contain three runs. Fixture
setup and provider downloading are excluded. CLI `elapsed_ms` includes downloading and
persistence; stored `preparation_ms` excludes result persistence. Reproduce with
`make bench-reconciliation`. These small local experiments are not capacity estimates, p99
measurements, payment throughput, or a claim that 10,000 payments settle in 189 ms.

Three hot-account experiments each attempted 200 fully affordable spends. At 50 writers,
`FOR UPDATE` carried 200/200 every time at 700–1,340 spends/s (sample p99 40.4–128.5 ms).
Bounded `SERIALIZABLE` carried 137–144/200 with 56–63 failed attempts at 228–355 spends/s.
This is a database ledger-debit experiment, not HTTP-to-provider settlement throughput;
contention is concentrated on one account, not spread uniformly. Only the row-lock path ships.
The trade is queued spending and a per-account throughput ceiling instead of retry exhaustion.
Since phase 08 the lock is the stored balance row, taken last in the transaction; the HTTP
sweep, hot-account and mixed-load numbers are in [BENCHMARKS.md](BENCHMARKS.md).

The Postgres rate limiter measured 0.171–0.253 ms per allowed request versus 0.101–0.144 ms
for `SELECT 1`; sequential samples, not paired latency measurements. Reproduce the recorded
experiments plus the new metrics benchmarks with `make bench` in a disposable database.

## Cards (V2, phase 09)

The account model for card spending. No authorization, holds or limit enforcement yet
(phase 11), and **no card numbers, CVVs or PINs, ever**: a card is an id (`card_<hex>`) and
four random cosmetic digits.

```text
POST /v1/cards                 Idempotency-Key required
     {"account_id": "acc_...", "per_transaction_limit": 50000, "daily_limit": 200000}
     -> 201 card   (same key + same body -> 201 same card; different body -> 422)
GET  /v1/cards/{id}            -> 200 | 404
POST /v1/cards/{id}/freeze     active -> frozen
POST /v1/cards/{id}/unfreeze   frozen -> active
POST /v1/cards/{id}/close      active | frozen -> closed (terminal)
```

- The funding account is the cardholder's **available** balance. Its first card also
  creates a **held** account (an ordinary asset account with a stored balance), shared by
  all its cards; phase 11 moves authorized amounts from available to held. Two concurrent
  first cards create exactly one held account: the held account is inserted by the same
  statement that wins the `card_accounts` row, so the loser creates nothing
  ([TestConcurrentFirstCardsShareOneHeldAccount](internal/api/cards_test.go)).
- The funding account must exist, be USD, and be neither a settlement account nor a held
  account (400).
- Each transition is one guarded `UPDATE ... WHERE status = ANY(allowed)`, never a read then
  a write, so a freeze racing a close cannot reopen a closed card
  ([TestConcurrentFreezeAndCloseEndClosed](internal/api/cards_transitions_test.go)).
  Repeating a transition already applied answers 200 with no new event; a forbidden one
  (for example unfreezing a closed card) answers `409 invalid_card_state`.
- Issue and every transition write `card.issued`, `card.frozen`, `card.unfrozen` or
  `card.closed` to the outbox (topic `cards`) in the same transaction; an outbox failure
  rolls the change back.

The cost is one extra account and balance row per funding account, and card idempotency
keys kept forever, as for transfers.

## Card network simulator (V2, phase 10)

We are the card issuer; the card network decides our inputs. `mock-network` plays that
network, the way MockBank plays the bank, and is written to a contract:
[docs/mock-network-api.md](docs/mock-network-api.md). No issuer endpoint exists yet
(phase 11); the tests use a fake issuer.

- It sends signed authorizations, reversals and advices to `ISSUER_URL`
  (`X-Network-Timestamp` / `X-Network-Signature`, the MockBank HMAC scheme with
  `NETWORK_SECRET`).
- **Deadline, retry, stand-in:** no answer within `NETWORK_AUTH_DEADLINE` → one retry with
  the same `auth_id`; still none → the network approves up to `NETWORK_STANDIN_LIMIT` on its
  own and later sends an advice the issuer must honour.
- Reversals and advices are retried on any non-2xx (1s … 16s, six attempts).
- Ten seeded scenarios (`POST /sandbox/scenarios`): approve and clear, tip, partial clear,
  duplicate, timeout then retry, stand-in, reversal before its authorization, incremental,
  offline purchase, late refund. **Same seed, same bytes**
  ([TestSameSeedSameBytes](internal/cardnetwork/scenario_test.go)).
- The clearing file (`GET /clearing?date=YYYY-MM-DD`, or `-clearing-dir` on the
  `scenario` subcommand) is built from the network's own journal: tips and partial amounts,
  reversed authorizations absent, offline lines with an empty `auth_id`, refunds weeks later.

```sh
make mock-network                                                        # serve on NETWORK_ADDR
make mock-network ARGS="scenario -name stand_in -card card_x -amount 5000 -seed 3 -clearing-dir /tmp/clearing"
```

`internal/cardnetwork` is a library so later phases drive it in-process with a fake clock.
The cost is the MockBank credibility gap again (we wrote both sides, so only the failure
modes we thought of are tested) and an in-memory journal that a restart loses.

## Honest V1 limits

- Authentication, not tenancy/authorisation: a valid API key can operate on any account.
- MockBank is a controlled simulator, not a real provider integration. Restarting it loses
  payment state and provider-side idempotency history.
- Kafka storage is not pinned to a Compose-managed named data volume; the image creates
  anonymous mounts. Container replacement is not a tested broker-durability guarantee.
- At-least-once transport, not universal exactly-once execution: consumers may run again.
  The SIGKILL proof covers reference persistence followed by a crash before offset commit;
  the earlier bank-acceptance/local-persistence window relies on the provider's stable client
  reference and retained lookup/deduplication state, not a cross-system transaction.
- Spendable balances are stored in `account_balances` and maintained by `ledger.Record`, the
  single write path; a CHECK refuses overdrafts and `ledger.VerifyBalances` compares every
  stored balance with its entries (run after every benchmark step). Administrator raw SQL
  that writes entries without the matching balance update **will drift** them; nothing in
  the database prevents that. Settlement accounts keep no stored balance (every payment
  touches them) and so cannot be a transfer source.
- Spending from one account is serialised on its balance row: one hot account has a
  per-account throughput ceiling. Past it, requests are refused fast with `503
  account_busy` (an in-process cap of 4 concurrent creations per account with a 100 ms wait,
  and a 250 ms lock timeout) instead of queueing; clients retry with the same
  Idempotency-Key. The cap is per process, so N API replicas admit 4N per account.
  Every `/v1/` request has a 5 s deadline; the auth and rate-limit middleware still answer
  a timeout with 500.
- One configured customer endpoint, signed retries and a dead status; no manual replay API
  or delivery history. Customers must deduplicate `event_id` from the signed JSON envelope
  after lost acknowledgements, not trust the event-ID header alone.
- A card's held account is an ordinary asset account: `POST /v1/transfers` does not yet
  refuse it as a source or destination. Card limits are stored, not enforced, until
  authorization (phase 11). There is no card listing endpoint.
- No automatic money repair, durable reconciliation scheduler, production DB roles, or
  production monitoring/deployment claim. Apply migrations and deploy new binaries explicitly.

V1 is one currency (USD), one simulated rail, API-key authentication, Postgres rate limiting,
ledger/concurrency correctness, Kafka/outbox, inbound/outbound webhooks, reconciliation,
minimal metrics, and measured failure evidence. Dashboard, Chaos API, manual replay and
notification history were cut. Real providers, FX, stablecoins, Redis/load testing at scale,
Kubernetes, fraud/KYC, tracing and multi-tenancy remain outside V1. See the scope fence in
[PLAN.md](PLAN.md); no additional product feature is needed to close this version.
