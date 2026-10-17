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
| One transfer (or card) per API key + idempotency key, including concurrent retries. | [TestIdempotentRequestsCreateOneTransfer](internal/api/idempotency_test.go), [TestRetryAfterFundsAreExhaustedReplaysTheOriginal](internal/api/idempotency_test.go), [TestIssueCardIsIdempotent](internal/api/cards_test.go); one answer and one hold per network `auth_id`: [TestSameAuthIDAlwaysGetsTheSameAnswer](internal/api/network_test.go) |
| Concurrent requests cannot overspend an account. | [TestConcurrentTransfersCannotOverspend](internal/api/overspend_test.go), [TestLedgerRefusesOverdraft](internal/ledger/stored_balance_test.go), [TestSecondSpendWaitsForTheFirstAndSeesItsDebit](internal/api/overspend_test.go), [TestStoredBalanceTracksEveryMovement](internal/ledger/stored_balance_test.go), [TestTwoTapsAtOnceCannotOverspend](internal/api/network_test.go) |
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
32 bytes in `MOCKBANK_WEBHOOK_SECRET`, `CUSTOMER_WEBHOOK_SECRET` and `NETWORK_SECRET` (the
card network's, shared with `mock-network`), plus a controlled
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

The account model for card spending (authorization and limit enforcement: phase 11
below), and **no card numbers, CVVs or PINs, ever**: a card is an id (`card_<hex>`) and
four random cosmetic digits.

```text
POST /v1/cards                 Idempotency-Key required
     {"account_id": "acc_...", "per_transaction_limit": 50000, "daily_limit": 200000,
      "blocked_mccs": ["7995"]}                    (optional, 4-digit merchant category codes)
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
[docs/mock-network-api.md](docs/mock-network-api.md). Its own tests use a fake issuer; the
API's tests drive it against the real issuer endpoints (phase 11).

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

## Card authorization (V2, phase 11)

The network asks "may `card_X` spend $50 at Corner Coffee?" and waits 2 s per attempt. The
API is the issuer side of [docs/mock-network-api.md](docs/mock-network-api.md):

```text
POST /network/authorizations   signed by the network (NETWORK_SECRET), no API key -> 200 answer | 503 no answer
POST /network/advices          a stand-in approval to honour                      -> 200 | 5xx (redelivered)
```

- **Approve** = move the amount from the card's funding (available) account to its held
  account in one ledger transaction. Declines move nothing and are stored too: unknown card,
  card not active, not USD, over the per-transaction limit, blocked merchant category
  (`blocked_mccs` on `POST /v1/cards`), over the daily limit, or insufficient funds.
  Incremental authorizations are phase 12, below.
- **Same `auth_id`, same answer.** `card_authorizations.auth_id` is the primary key and is
  inserted first; a duplicate waits for the original to commit and answers the stored row.
  Every answer is the stored row
  ([TestSameAuthIDAlwaysGetsTheSameAnswer](internal/api/network_test.go)).
- **Two taps cannot overspend.** The hold is written last; the `account_balances` CHECK
  refuses an overdraft and rolls back the row, the event and the daily spend. A second short
  statement then stores the decline (`insufficient_funds`), or answers whatever a concurrent
  duplicate stored ([TestTwoTapsAtOnceCannotOverspend](internal/api/network_test.go)).
- **Daily limit** is a counter row per card and UTC day, checked and incremented in one
  conditional upsert, never read then written
  ([TestConcurrentTapsRespectTheDailyLimit](internal/api/network_test.go)).
- **Deadline:** each request gets 1 s. A busy account (4 in flight per account in this
  process, 250 ms lock timeout) or a missed deadline answers `503`, which the network counts
  as no answer: it retries once with the same `auth_id`, then stands in.
- **Stand-in is honoured.** An advice (deduplicated on `advice_id` in `network_messages`, in
  the same transaction as its effect) approves the authorization whatever we answered, even
  for a frozen card or past its limits, and holds `min(amount, available)`. What could not
  be held is stored as `hold_shortfall`, for reconciliation (phase 15): never an overdraft
  ([TestStandInAdviceIsHonoured](internal/api/network_test.go)).
- The simulator drives the real handler end to end and the network's journal agrees with
  our rows ([TestNetworkScenariosAgainstTheIssuer](internal/api/network_scenarios_test.go)).
- A held account cannot be a transfer source or destination (400).

Measured in [BENCHMARKS.md](BENCHMARKS.md) (phase 11). The cost is a second hot row per card
(the daily counter, locked until commit), a second short transaction on the
insufficient-funds path, and stand-in shortfalls that are real credit exposure we only
detect, never fix.

## Reversals, expiry and incremental holds (V2, phase 12)

Money held by an authorization must never be stuck and never be released twice.

```text
POST /network/reversals        signed; release (part of) a hold -> 200 released | nothing_held | pending | already_processed
make hold-expirer              release approved holds older than HOLD_EXPIRY_DAYS
```

- **One release primitive** (`holds.Release` in [internal/holds](internal/holds/holds.go)):
  lock the authorization row, release `min(amount, held_amount)`, give that back to the
  day's spend counter, outbox event, ledger `held -> available` last. Every reversal,
  parked reversal and expiry goes through it, so the row lock and `held_amount` make a
  release exactly-once; the held account's balance CHECK is the backstop
  ([TestReleaseNeverReleasesMoreThanHeld](internal/holds/holds_test.go),
  [TestDifferentReversalsNeverReleaseMoreThanHeld](internal/api/reversals_test.go)).
- **Reversals** are deduplicated on `reversal_id` in `network_messages`, in the same
  transaction as their effect ([TestReversalReleasesTheHoldOnce](internal/api/reversals_test.go)).
  One for an incremental `auth_id` releases from its chain's initial row.
- **Reversal before the authorization:** parked in `pending_reversals` and applied by the
  approval (or a stand-in advice) in the same transaction as the hold. The answer stays
  "approved" and the ledger shows hold then release. A per-`auth_id` transaction-scoped
  advisory lock, taken first by both sides, closes the window in which each commits
  without seeing the other
  ([TestReversalBeforeAuthorizationIsAppliedOnArrival](internal/api/reversals_test.go),
  [TestReversalBeforeAuthScenario](internal/api/network_scenarios_test.go)). A reversal of
  a declined authorization is parked too: a stand-in advice can still approve it.
- **Expiry:** `holds.ExpireBatch` claims one open hold at a time with
  `FOR UPDATE SKIP LOCKED` on a partial index of open holds and releases it in full in its
  own short transaction, so concurrent expirers and reversals never release the same money
  twice and an account's balance lock is never held across a batch
  ([TestExpiryReleasesOldHoldsOnce](internal/holds/expire_test.go),
  [TestReversalRacingExpiryReleasesOnce](internal/api/reversals_test.go)). A clearing line
  that arrives after its hold expired is phase 13's problem.
- **Incremental** (`kind = incremental`, amount = the increase): the original row is locked,
  the chain total is checked against the per-transaction limit under that lock, the day's
  counter and the ledger take the increase; the original's `approved_amount` and
  `held_amount` grow, the incremental row holds nothing. Declines: `original_not_found`,
  `original_not_approved`, `original_expired`, `card_mismatch`, plus the card rules
  ([TestIncrementalRaisesTheOriginalHold](internal/api/incremental_test.go),
  [TestIncrementalDeclines](internal/api/incremental_test.go),
  [TestConcurrentIncrementalsRespectFundsAndLimits](internal/api/incremental_test.go)).

Measured in [BENCHMARKS.md](BENCHMARKS.md) (phase 12). The cost is a pending-reversal table
that grows with orphaned reversals (no retention yet), one more row lock on the
incremental and reversal paths, an advisory lock and a pending-reversal read in every
approval's first batch, and an expirer process to run.

## Clearing (V2, phase 13)

A tap only holds money. Days later the network's clearing file says what each merchant
actually took, and clearing turns holds into final charges. The file is the record we must
post: anything odd is flagged, never silently fixed, and no account is overdrawn.

```text
make clearing ARGS="-download $NETWORK_URL -date 2026-10-18"   GET /clearing?date= from the network, import
make clearing ARGS="-file clearing-2026-10-18.csv -date 2026-10-18"
make clearing ARGS="-list"                                     imported files
make clearing ARGS="-inspect clf_… -flag shortfall"            a file's summary and lines
make clearing ARGS="-post-deferred"                             post refunds older imports deferred
```

- **Validate, then store once** ([internal/clearing](internal/clearing/import.go)): the whole
  file is parsed first (exact header, positive integer amounts, USD, UTC timestamps on the
  file's date, no `clearing_id` twice); one bad line rejects the file and writes nothing
  ([TestInvalidFileWritesNothing](internal/clearing/import_test.go)). The raw bytes are kept
  with their SHA-256; the same bytes for the same date find the stored file and, if every
  line has a record, process nothing
  ([TestClearingSameFileTwiceChangesNothing](internal/clearing/import_test.go)). A file is
  keyed on `(file_date, sha256)`: a header-only file is the same bytes every quiet day
  ([TestHeaderOnlyFilesForDifferentDatesAreDistinct](internal/clearing/import_test.go)).
- **One short transaction per line**, keyed by the network's `clearing_id`: the record is
  inserted first, so a second importer of the same line waits and skips; then the
  `auth_id` lock and the authorization row, the daily counter, the balances, and the ledger
  last, in the authorization path's lock order. A crash mid-file is finished by running it
  again ([TestClearingCrashMidFileRerunPostsEachLineOnce](internal/clearing/import_test.go),
  [TestClearingConcurrentImportsOfOneFile](internal/clearing/import_test.go)).
- **Posting** (`F` cleared, `H` still held; one balanced ledger transaction per line, credit
  `acc_network_payable_usd`, what we owe the network):

  | line | money | flag |
  |---|---|---|
  | `F == H` | held `F` → payable | |
  | `F < H` (partial shipment) | held `F` → payable, `H − F` back to available, daily counter freed | |
  | `F > H` (tip) | held `H` + available `F − H` → payable | `over_tolerance` if `F − H` > `CLEARING_OVER_TOLERANCE_PCT` of the authorized amount |
  | hold expired / reversed / already cleared | available `F` → payable | `cleared_after_expiry` / `cleared_after_reversal` / `duplicate_clearing` |
  | no or unknown `auth_id` (offline) | available `F` → payable | `unauthorized_clearing` |
  | unknown card | nothing, status `rejected` | `unknown_card` |
  | not enough available | `min(want, balance)`, rest recorded as `shortfall` | `shortfall` |
  | refund | see [Refunds](#refunds-v2-phase-14) | |

  ([TestClearingExactPartialAndTip](internal/clearing/post_test.go),
  [TestShortfallNeverOverdraws](internal/clearing/post_test.go),
  [TestClearingRacingReversalAndExpiry](internal/clearing/post_test.go): clearing, reversals
  and the expirer racing for 40 holds never both release and charge one.)
- **End to end:** every simulator scenario against the real issuer handler, its clearing
  files imported twice: 9 purchases posted, the late refund credited back, nothing left
  held, 0 drift ([TestNetworkScenariosClearEndToEnd](internal/clearing/network_test.go)).

Measured in [BENCHMARKS.md](BENCHMARKS.md) (phase 13). The cost is that a crash leaves a
file partly posted until it is run again, that one importer posts ~1,300 lines/s serially,
and that charging the network's amount when it exceeds the hold, or when nothing was held,
is exposure we flag rather than refuse.

## Refunds (V2, phase 14)

A customer returns shoes three weeks later; the network's clearing file carries a `refund`
line. A refund is **new money back**, a new ledger transaction, never an edit of the charge.

- **Same line pattern as purchases** ([internal/clearing/refund.go](internal/clearing/refund.go)):
  record claimed first by `clearing_id`, the `auth_id` lock and authorization row, then
  one balanced transaction last: debit `acc_network_payable_usd` (we owe the network less),
  credit the card's available account; `card.refund.posted` in the outbox. Holds and the
  daily counter are not touched; a closed card's account is still credited.

  | refund line | money | flag |
  |---|---|---|
  | names a cleared purchase, total refunded ≤ cleared | payable → available, `refunded_amount += F` | |
  | takes refunds past what the purchase cleared | the same, still credited | `refund_exceeds_purchase` |
  | no `auth_id`, unknown `auth_id`, or never cleared | payable → available | `unmatched_refund` |
  | names another card's purchase | the line's card is credited | `card_mismatch` |
  | unknown card | nothing, status `rejected` | `unknown_card` |

  ([TestRefundCreditsAvailableWithoutTouchingTheCharge](internal/clearing/refund_test.go),
  [TestPartialAndRepeatedRefunds](internal/clearing/refund_test.go),
  [TestUnmatchedRefundIsPostedAndFlagged](internal/clearing/refund_test.go),
  [TestRefundLineImportedTwiceCreditsOnce](internal/clearing/refund_test.go),
  [TestLateRefundScenarioEndToEnd](internal/clearing/network_test.go).)
- **Refunds deferred by phase 13 imports:** `clearing.PostDeferred` (`-post-deferred`)
  claims each with `UPDATE … WHERE status = 'deferred'` in the line's own transaction, so
  concurrent or repeated runs credit it once
  ([TestPostDeferredIsIdempotent](internal/clearing/deferred_test.go)).

Measured in [BENCHMARKS.md](BENCHMARKS.md) (phase 14). The cost is that we credit what the
network sends even past the purchase (flagged exposure, not refused), and refund matching is
only as good as the network's `auth_id`.

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
- Holds are released by reversals and expiry (phase 12) and turned into charges by
  clearing (phase 13); refunds credit them back (phase 14). Nothing pays the network
  payable yet (phase 15). Stand-in and clearing shortfalls are recorded, not
  chased. There is no card or authorization listing endpoint.
- No automatic money repair, durable reconciliation scheduler, production DB roles, or
  production monitoring/deployment claim. Apply migrations and deploy new binaries explicitly.

V1 is one currency (USD), one simulated rail, API-key authentication, Postgres rate limiting,
ledger/concurrency correctness, Kafka/outbox, inbound/outbound webhooks, reconciliation,
minimal metrics, and measured failure evidence. Dashboard, Chaos API, manual replay and
notification history were cut. Real providers, FX, stablecoins, Redis/load testing at scale,
Kubernetes, fraud/KYC, tracing and multi-tenancy remain outside V1. See the scope fence in
[PLAN.md](PLAN.md); no additional product feature is needed to close this version.
