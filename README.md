# payments-platform

> **The backend for a debit card. When someone taps their card, it decides yes or no within
> a fraction of a second, holds the money, and settles the final amount days later, without
> ever losing or double-charging a cent.**

We are the card issuer. A simulated card network (written by us, to a
[contract](docs/mock-network-api.md)) asks for decisions, retries, stands in when we are
slow, sends reversals out of order and a clearing file days later. Underneath is a
double-entry, append-only ledger in PostgreSQL with stored balances guarded by a `CHECK`,
first built for bank transfers (V1, still here, [below](#v1-transfers-through-an-external-bank)).

Go 1.26, PostgreSQL 16, Kafka, Docker Compose. A modular monolith plus workers. Amounts are
integer cents. **Status:** V1 (transfers, phases 1–7) and V2 (debit card, phases 8–16) are
merged. [ROADMAP.md](ROADMAP.md) has the plan and phase log, [BENCHMARKS.md](BENCHMARKS.md)
every number, [PLAN.md](PLAN.md) the decisions and build log.

## The card flow

```text
 tap ─▶ POST /network/authorizations  (signed, 1 s budget, network waits 2 s)
          card rules, daily limit, hold: available ──▶ held       one transaction
          answer = the stored row; same auth_id ⇒ same answer
   │      no answer ⇒ network retries once, then stands in ──▶ advice ⇒ we hold anyway
   │
   ├─ reversal (maybe before its authorization) ── held ──▶ available
   ├─ expiry after HOLD_EXPIRY_DAYS (expirer)   ── held ──▶ available
   ├─ incremental (hotel, fuel)                 ── available ──▶ held, on the original
   │
 days later: clearing file ─▶ clearing import, one transaction per line
          held ──▶ network payable   (tip: + available; partial: rest back to available)
          refund line: network payable ──▶ available (new money, never an edit)
   │
 card-settle:   network payable ──▶ network cash, once per day
 card-reconcile: our records × clearing file × network statement ─▶ findings (never fixes)
```

Every money arrow is a balanced ledger transaction; a hold is not a flag on a row, it is money
moved to the cardholder's held account, so the ledger's guarantees cover it.

## Guarantees, each with its proof

| Guarantee | Named automated proof |
| --- | --- |
| One answer, one row, one hold per network `auth_id`, however many copies arrive at once. | [TestSameAuthIDAlwaysGetsTheSameAnswer](internal/api/network_test.go), [TestDuplicateAuthorizationsUnderLoad](internal/api/chaos_test.go) |
| Two taps cannot overspend; the daily limit holds under concurrency. | [TestTwoTapsAtOnceCannotOverspend](internal/api/network_test.go), [TestConcurrentTapsRespectTheDailyLimit](internal/api/network_test.go), [TestConcurrentIncrementalsRespectFundsAndLimits](internal/api/incremental_test.go) |
| A stand-in approval is honoured and never overdraws (the shortfall is recorded). | [TestStandInAdviceIsHonoured](internal/api/network_test.go) |
| A hold is released at most once, whatever races (reversals, expiry, clearing). | [TestReleaseNeverReleasesMoreThanHeld](internal/holds/holds_test.go), [TestDifferentReversalsNeverReleaseMoreThanHeld](internal/api/reversals_test.go), [TestReversalRacingExpiryReleasesOnce](internal/api/reversals_test.go), [TestClearingRacingReversalAndExpiry](internal/clearing/post_test.go) |
| A reversal that arrives before its authorization is never lost. | [TestReversalBeforeAuthorizationIsAppliedOnArrival](internal/api/reversals_test.go), [TestReversalBeforeAuthorizationUnderLoad](internal/api/chaos_test.go) |
| A killed API process leaves no hold without its row, no row without its hold, and agrees with the network. | [TestAPIKilledMidAuthorizationLeavesNoPartialHold](cmd/api/kill_test.go) |
| Each clearing line is posted once: same file twice, concurrent importers, crash and rerun. | [TestClearingSameFileTwiceChangesNothing](internal/clearing/import_test.go), [TestClearingConcurrentImportsOfOneFile](internal/clearing/import_test.go), [TestClearingCrashMidFileRerunPostsEachLineOnce](internal/clearing/import_test.go) |
| Clearing never overdraws; what is missing is recorded as a shortfall. | [TestShortfallNeverOverdraws](internal/clearing/post_test.go) |
| A refund is credited once, as a new transaction. | [TestRefundLineImportedTwiceCreditsOnce](internal/clearing/refund_test.go), [TestRefundCreditsAvailableWithoutTouchingTheCharge](internal/clearing/refund_test.go) |
| The network is paid once per day. | [TestSettleRecordsTheDayOnce](internal/cardsettlement/settle_test.go) |
| Reconciliation detects every defined exception class and changes no money. | [TestEveryClassIsDetected](internal/cardrecon/recon_test.go), [TestCleanDayIsAllMatched](internal/cardrecon/recon_test.go) |
| Every committed ledger transaction balances; history rejects UPDATE/DELETE. | [TestRecordMovesMoneyAndBalances](internal/ledger/record_test.go), [TestRecordRejectsOverflowingTotals](internal/ledger/overflow_test.go), [TestLedgerHistoryRejectsUpdatesAndDeletes](internal/ledger/immutable_test.go) |
| Stored balances never go negative and always equal their entries. | [TestLedgerRefusesOverdraft](internal/ledger/stored_balance_test.go), [TestStoredBalanceTracksEveryMovement](internal/ledger/stored_balance_test.go); `VerifyBalances` after every benchmark step |
| One transfer (or card) per API key + idempotency key, including concurrent retries. | [TestIdempotentRequestsCreateOneTransfer](internal/api/idempotency_test.go), [TestIssueCardIsIdempotent](internal/api/cards_test.go) |
| Concurrent transfers cannot overspend. | [TestConcurrentTransfersCannotOverspend](internal/api/overspend_test.go) |
| Duplicate provider webhooks and Kafka redelivery have one financial effect. | [TestConcurrentDuplicateWebhooksHaveOneEffect](internal/api/webhooks_test.go), [TestSenderSIGKILLRedeliversWithoutDuplicatePayment](internal/consumer/crash_test.go), [TestKafkaRedeliveryCreatesOneDurableJob](internal/customerwebhooks/worker_test.go) |
| Every financial transition and card change is audited in its own transaction. | [TestEveryFinancialTransitionRetainsAuditEvidence](internal/api/audit_test.go), [TestEveryCardChangeIsAuditedInItsTransaction](internal/api/cards_transitions_test.go) |
| Transfer reconciliation detects all six outcomes without mutating money. | [TestProviderDiscrepanciesAreDetectedWithoutFinancialMutation](internal/reconciliation/evidence_test.go) |

These cover the implemented API and worker paths, not an administrator with raw SQL: a
trusted role can still TRUNCATE, disable triggers, or write entries without balances.

## Authorization latency

Open model (k6 `constant-arrival-rate`: arrivals do not wait for responses, so a slow
server shows as latency and dropped iterations, not hidden by coordinated omission), signed
network requests against the real `cmd/api` binary, one process, **pool of 10 Postgres
connections**, laptop (Apple M4, Postgres in Docker, k6 on the same machine), 30 s steps
after a discarded 15 s warm-up, median of 3 runs. Taps spread over 1,001 cards:

| offered /s | achieved /s | p50 ms | p95 ms | p99 ms | non-200 |
|---:|---:|---:|---:|---:|---|
| 250 | 250 | 2.44 | 4.05 | 11.98 | 0 |
| 500 | 500 | 1.39 | 2.40 | **9.44** | 0 |
| 1,000 | 1,000 | 1.32 | 7.97 | **68.24** | 0 |
| 1,500 | 1,439 | 1,000.89 | 1,005.33 | 1,022.76 | 0 / 39,822 / 42,057 (per run) |

- **Knee: ~1,000 authorizations/s** with p99 under the 100 ms target. Past it, goodput
  collapsed (requests waited out the 1 s deadline and answered 503); since phase 17 the
  excess is refused in milliseconds and approvals level off (1,350–1,900/s offered 3,000,
  [BENCHMARKS.md](BENCHMARKS.md#phase-17-shed-load-early-cache-api-keys-2026-10-18)).
- **One hot card: ~500 approvals/s** clean; offered 1,000/s it approves 517–965/s and
  refuses the rest fast with `503 account_busy`, because spending from one account is
  serialised on its balance row.
- **A full card day** (500 taps/s + reversals + a hot card, while a 10,000-line clearing
  import and 2,000 expiries run): tap p99 6 ms before, **43 ms during** the batch jobs,
  0 non-200; then settled, reconciled (9,940 MATCHED + 60 offline purchases, as expected)
  and 0 drift. `make bench-day`.

These runs shared the laptop with a video call; same-day runs earlier, on battery, were
faster (p99 4–13 ms at 500, 5–16 ms at 1,000). Laptop numbers are for comparing commits, not
capacity claims. Hot-card, transfer and per-run tables: [BENCHMARKS.md](BENCHMARKS.md#phase-16-v2-benchmark-and-failure-evidence-2026-10-17).

## Failure proofs

Each one was first shown to fail with the mechanism it relies on removed (quoted in
BENCHMARKS.md).

- [TestDuplicateAuthorizationsUnderLoad](internal/api/chaos_test.go): 30 auth_ids × 20
  concurrent copies while other taps hit the same cards → one row, one hold, one event each
  (without the primary key: 20 of each).
- [TestReversalBeforeAuthorizationUnderLoad](internal/api/chaos_test.go): 200 reversals
  delivered before their authorization and 200 racing it, under load → each released once,
  none parked forever.
- [TestAPIKilledMidAuthorizationLeavesNoPartialHold](cmd/api/kill_test.go): SIGKILL the real
  API mid-load, restart; the simulator retries, stands in and advises → every network
  decision matches our row, every approved row has its hold and vice versa, 0 drift.
- [TestClearingSameFileTwiceChangesNothing](internal/clearing/import_test.go): the same
  clearing file imported twice posts nothing the second time.
- [TestSenderSIGKILLRedeliversWithoutDuplicatePayment](internal/consumer/crash_test.go) (V1):
  a Kafka consumer killed before its offset commit does not pay twice.

## Honest limits

- **Laptop numbers**, k6 on the same machine, one API process, one Postgres with default
  settings. No claim about production capacity.
- **The network is a simulator we wrote.** It tests the failure modes we thought of; a real
  network's edge cases are not covered. No PCI scope, no card numbers (a card is an id and
  four cosmetic digits), no real network or bank connection.
- **Single Postgres** is the only source of truth and the scaling limit. Scaling path when
  measured, not before: more API replicas (stateless), then a bigger pool / PgBouncer, then
  partitioning accounts across databases.
- **Per-process admission gate.** The 4-in-flight-per-account cap lives in each API process:
  N replicas admit 4N per account (the balance row lock still prevents overspend).
- **Load shedding is per process and refuses a little early** (phase 17). Each API process
  admits 3 × its pool size of taps, advices, reversals and transfer creations in flight
  and waits at most 20 ms for a connection; past that it answers `503 overloaded` at once.
  Offered 3,000 taps/s it keeps approving 1,350–1,900/s instead of collapsing to 60–860/s,
  but a stall below the knee also refuses a few (0.05–0.4 % at 1,000/s) that would have
  been served late; the network retries once and then stands in. Requests waiting at the
  per-account gate hold admission slots, so one very hot card fills them sooner.
- **Revoked API keys keep working for up to 30 s** on processes other than the one that
  revoked them: verified keys are cached in memory for 30 s. Unknown keys are not cached
  and still cost a query each.
- **The rate limiter counts in Redis and fails open.** Each check is one atomic script
  under a 50 ms deadline on a pre-warmed connection pool; if Redis is down or slower than
  that, the request is served
  unchecked and counted in `payments_ratelimit_failopen_total`, so quotas are not enforced
  during a Redis outage (admission and the bounded pool still protect the database). On
  the laptop under load the limiter's share of a transfer fell from 24–35 % to 12–17 %
  (0.2–0.4 ms per check through Docker Desktop), not the < 5 % targeted, and 0–0.13 % of
  checks below the knee still timed out and failed open during machine-wide stalls
  ([BENCHMARKS.md](BENCHMARKS.md#phase-18-rate-limiter-in-redis-2026-10-22)).
  `RATE_LIMIT_BACKEND=postgres` keeps the old limiter. Replicas take the window from their
  own clocks, so skew can count a request into the adjacent window at a boundary.
- **Reconciliation holds a day in memory** (~2.9 KB per record; 100,000 lines ≈ 290 MB).
  `HOLD_AFTER_CLEARING` compares transaction start times, so an incremental that started
  before the clearing transaction but waited on its lock is not caught; a late advice is
  only caught by reconciling the date again.
- Stand-in and clearing shortfalls are recorded and reported, never chased. Findings are
  only as complete as the classes we defined. Append-only tables have no retention.
- API keys authenticate but do not authorise: any key can act on any account. No card or
  authorization listing endpoint. MockBank and the network simulator keep state in memory.

## Running it

Needs Docker, Go 1.26, the golang-migrate v4 CLI (Postgres driver) and, for benchmarks, k6.

```sh
test -f .env || cp .env.example .env      # then set the secrets below
make up && make migrate-up                # Postgres :5433, Kafka :9092, MockBank :8081
make run                                  # API
make mock-network                         # card network simulator, calls ISSUER_URL
make hold-expirer | make clearing ARGS=… | make card-settle ARGS=… | make card-reconcile ARGS=…
```

`.env` needs `DATABASE_URL` and distinct random secrets of at least 32 bytes in
`MOCKBANK_WEBHOOK_SECRET`, `CUSTOMER_WEBHOOK_SECRET` and `NETWORK_SECRET` (shared with
`mock-network`), plus `CUSTOMER_WEBHOOK_URL`. Secrets live only in the ignored `.env`.
Every command fails rather than guess a missing setting. The pool size is the DSN
parameter `pool_max_conns`.

```sh
make test                                 # go test ./... in per-package test_* schemas
go test -race ./... -count=1
make bench-http                           # HTTP sweep, payments_bench DB only
MODES="auth-spread auth-hot" RATES="250 500 1000 1500" make bench-http
make bench-day                            # card day under load, payments_bench_day DB only
make chaos-kill-consumer                  # V1 Kafka crash proof
```

Benchmarks drop and recreate only databases named `payments_bench*`. Tests use
package-specific `test_*` schemas (or `TEST_DATABASE_URL`); never point either at data you
want to keep. The full suite also needs the local Kafka broker.

## The card backend in detail

### Cards (phase 9)

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

### Card network simulator (phase 10)

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

### Card authorization (phase 11)

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

### Reversals, expiry and incremental holds (phase 12)

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

### Clearing (phase 13)

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
  | refund | see [Refunds](#refunds-phase-14) | |

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

### Refunds (phase 14)

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

### Network settlement and card reconciliation (phase 15)

Each day we owe the network the net of what cleared: purchases minus refunds. We pay it
once, and prove every cent three ways: our records (authorizations, holds, posted clearing
lines), the clearing file as those records reflect it, and what we paid against the
network's settlement statement (`GET /settlement?date=`, [contract](docs/mock-network-api.md#settlement-statement)).
As in V1: **detect and report, never fix money automatically.**

```text
make card-settle ARGS="-date 2026-10-18"                                     pay the day, once
make card-reconcile ARGS="-download $NETWORK_URL -date 2026-10-18 -run-id crn_1018"
make card-reconcile ARGS="-statement settlement-2026-10-18.csv -date 2026-10-18 -run-id crn_1018"
make card-reconcile ARGS="-list"
make card-reconcile ARGS="-inspect crn_1018 -class LATE_CLEARING"
```

- **Pay once** ([cardsettlement.Settle](internal/cardsettlement/settle.go)): one transaction
  inserts the `network_settlements` row first, totals computed in the same statement from
  the date's posted clearing records; the date is the primary key, so a concurrent or
  repeated settlement waits, inserts nothing and returns the first row. Net > 0: debit
  `acc_network_payable_usd`, credit `acc_network_cash_usd`; net < 0 the reverse; net 0 no
  ledger transaction. A date with no clearing file, or with deferred lines, is refused rather
  than locked in at a wrong total
  ([TestSettleRecordsTheDayOnce](internal/cardsettlement/settle_test.go): 20 concurrent
  settlements → 1 row, 1 ledger transaction, payable back to 0).
- **Reconcile** ([cardrecon.Reconcile](internal/cardrecon/run.go)): one read-only Repeatable
  Read snapshot of the date's clearing records, the authorizations they clear (and their
  incrementals), holds open longer than `HOLD_EXPIRY_DAYS`, holds expired that day, and the
  recorded settlement; saved append-only (raw statement + SHA-256, counts, findings with
  evidence), with V1's run-ID replay: same ID and bytes return the saved run, different bytes
  are refused. MATCHED is counted, not stored.

  | class | from |
  |---|---|
  | `CLEARED_NOT_AUTHORIZED` | flags `unauthorized_clearing`, `unknown_card` |
  | `AUTHORIZED_NOT_CLEARED` | approved hold open longer than the expiry window, or expired that day, never cleared |
  | `AMOUNT_MISMATCH` | `over_tolerance` |
  | `DUPLICATE_CLEARING` | `duplicate_clearing` |
  | `LATE_CLEARING` | `cleared_after_expiry`, `cleared_after_reversal`, `nothing_held` |
  | `REFUND_EXCEPTION` | `refund_exceeds_purchase`, `unmatched_refund`, `card_mismatch` |
  | `FUNDING_SHORTFALL` | a line's `shortfall`, or an authorization's `hold_shortfall` |
  | `HOLD_AFTER_CLEARING` | a hold (advice or incremental) whose ledger transaction is later than the first posted purchase on its chain |
  | `SETTLEMENT_MISMATCH` | statement ≠ recorded settlement, posted records ≠ recorded settlement, or nothing recorded |

  ([TestEveryClassIsDetected](internal/cardrecon/recon_test.go): one corrupted day, every
  class, ledger and balances unchanged; [TestCleanDayIsAllMatched](internal/cardrecon/recon_test.go):
  all ten simulator scenarios end to end → 9 MATCHED + the offline purchase;
  [TestSettlementMismatchIsDetected](internal/cardrecon/recon_test.go),
  [TestReconRunReplayAndConflict](internal/cardrecon/recon_test.go),
  [TestFindingsAreAppendOnly](internal/cardrecon/recon_test.go).)

Measured in [BENCHMARKS.md](BENCHMARKS.md) (phase 15). The cost is two more append-only
tables growing per day with no retention, and that findings are only as complete as the
clearing flags and the statement: a class nobody thought of is not detected. The settlement
pays what was posted when it ran; a line posted for that date later is reported, not paid.

## V1: transfers through an external bank

The foundation the card backend sits on. A bank can accept a payment and then time out,
deliver a callback twice, or disagree with our records. The API must not guess that a
timeout means failure, refund money blindly, or turn a retry into a second payment.

```text
POST /v1/transfers + API key + Idempotency-Key
  -> Postgres transaction: intent + source-to-settlement ledger + outbox
  -> relay -> Kafka -> sender -> MockBank
  -> signed provider callback -> destination credit OR source refund + completion outbox
  -> relay -> Kafka -> durable customer delivery -> signed HTTP notification/retries
  -> provider CSV + internal snapshot -> reconciliation findings (detect, never auto-fix)
```

A transfer returns `processing`; it is not settled until a callback or resolving lookup
confirms its outcome. Spending is serialised on the source account's stored balance row,
taken last in the transaction; past an account's ceiling requests are refused fast with
`503 account_busy` (4 in flight per account per process, 100 ms wait, 250 ms lock timeout)
and clients retry with the same Idempotency-Key. Every `/v1/` request has a 5 s deadline.
A process with 3 × pool size transfer creations and taps in flight, or no connection free
within 20 ms, answers `503 overloaded` with `Retry-After: 1` and records nothing.

### End-to-end demonstration

`make demo-v1` runs [TestV1PaymentSurvivesTimeoutAfterSuccess](cmd/mock-bank/v1_demo_test.go)
in disposable schemas, loopback servers and unique Kafka topics; it does not touch the
running API, MockBank or application tables:

```text
$10 source fixture -> authenticated $5 transfer -> outbox -> Kafka -> sender -> MockBank
Timeout after success:  bank accepts/settles, acknowledgement withheld -> UNRESOLVED
Before callback:        one submission; source $5, destination $0; STATUS_MISMATCH detected
Duplicate signed callbacks -> one settlement; source $5, destination $5
Customer returns 503, then 200 -> same signed event, two attempts, delivered
Final reconciliation -> MATCHED; no automatic repair or second payment
```

The older `make demo` script **resets the configured application database**; do not run it
against retained data.

### Processes

Mint a key with `make apikey NAME="local dev"` (shown once; `make apikeys` lists prefixes).
`make run` serves the API, `make relay` publishes outbox rows, `make sender` submits
transfers, `make worker` is the polling safety net that resolves unknown outcomes,
`make webhook-sender` delivers customer notifications with durable retries, and
`make reconcile ARGS='-interval 24h'` reconciles daily. The sender and poller share one
guarded claim, so they never both pay a transfer. `/readyz` and `/healthz` are readiness
and liveness.

### Metrics

Authenticated `GET /metrics` (Prometheus text, no extra dependency): HTTP status-class
counters, transfer counts by status, outbox backlog, customer delivery counts, and the
latest reconciliation run's exception counts and time. Fixed labels only (20 series per
instance), no IDs. A scrape costs 0.5–1.0 ms with 1,000 rows of each kind; the middleware
16.9–23.5 ns per request (`make bench-metrics`). Database gauges are shared state: do not sum
them across replicas. Proofs in [internal/api/metrics_test.go](internal/api/metrics_test.go).

### Reconciliation

MockBank exposes `GET /settlements` ([CSV contract](docs/mockbank-api.md#reconciliation-snapshot)).

```sh
make reconcile ARGS='-report bank.csv -run-id run_001'      # same ID + bytes replays the run
make reconcile ARGS='-list'
make reconcile ARGS='-inspect run_001 -classification AMOUNT_MISMATCH'
```

Each run atomically keeps the raw CSV and its SHA-256, a read-only Repeatable Read snapshot,
both capture times, six finding counts and the evidence; persisted evidence rejects
UPDATE/DELETE. Read-only routes: `GET /v1/reconciliation/runs`, `/runs/{id}`,
`/runs/{id}/findings`. It only detects: it never settles, refunds, resubmits or repairs.
The provider and internal captures are not atomic across systems, so in-flight changes can
show as transient exceptions. 10,000 rows: parse + snapshot + compare + persist 164–270 ms
(`make bench-reconciliation`).

### V1 limits

- MockBank is a controlled simulator; restarting it loses payment state and provider-side
  idempotency history.
- At-least-once transport, not exactly-once execution: consumers may run again. The SIGKILL
  proof covers persistence followed by a crash before offset commit; the bank-acceptance
  window relies on the provider's stable client reference.
- One configured customer endpoint, signed retries and a dead status; no manual replay API.
  Customers must deduplicate `event_id` from the signed envelope.
- The recurring reconciliation command is process-local, with no durable schedule.
- Kafka storage is not on a Compose-managed named volume.
- The auth middleware and the Postgres rate limiter answer a timeout with 500, not 503
  (the Redis rate limiter fails open instead).
