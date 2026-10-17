# Roadmap after V1: a debit card backend

**Written:** 2026-10-13. **Builds on:** V1 in [PLAN.md](PLAN.md) (ledger, idempotency,
overspend protection, outbox + Kafka, signed webhooks, reconciliation). Nothing in V1 is thrown
away; every new piece sits on top of it.

This file replaces the "Later versions" list at the end of PLAN.md's scope fence. The working
method, the rules and the session LOG stay in PLAN.md.

## Progress

| Phase | Status | PR | Key number / proof |
|---|---|---|---|
| 8 fast spending path | merged 2026-10-17 | #10 | hot account ~330 -> 1,212 req/s accepted (same-session main vs p08, BENCHMARKS.md); balance read 0.12 ms at 1M entries |
| 9 cards | in review | #11 | 20 concurrent first cards -> 1 held account, 0 orphans; 20 same-key issues -> 1 card; freeze racing close never reopens a closed card |
| 10 network simulator | in review | #12 | 10 seeded scenarios, same seed -> byte-identical messages and clearing file; stand-in after two deadlines, advices/reversals retried six times then given up |
| 11 authorization | in review | #13 | 20 concurrent $30 taps on $50 -> 1 approved, 19 insufficient_funds; 20 duplicates of one auth_id -> 1 row, 1 hold; auth-spread p99 3.2 ms at 500/s, 34-57 ms at 1,000/s |
| 12 reversals, expiry, incremental holds | in review | #14 | 20 copies of one reversal -> 1 release; 10 x $20 reversals on a $50 hold -> $50 released; reversal racing its authorization (20 pairs) -> 0 stuck (1 stuck without the auth_id lock); 20 concurrent increments under a $100 chain limit -> exactly 8; expiry 1,400 holds/s |
| 13 clearing | in review | #15 | same file twice -> 0 changes; 4 concurrent importers of one file -> each line posted once; crash at line 3 of 5, rerun -> 5 posted once; clearing racing reversals and expiry on 40 holds -> 0 both released and charged; shortfall never overdraws; import 10,000 lines ~1,300/s, replay ~16 ms |
| 14 refunds | not started | | |
| 15 network settlement and reconciliation | not started | | |
| 16 benchmark and evidence | not started | | |

Dated entries per phase are in the [phase log](#phase-log) at the bottom.

---

## 1. The one-sentence answer

> **I built the backend for a debit card. When someone taps their card, it decides yes or no
> within a fraction of a second, holds the money, and settles the final amount days later,
> without ever losing or double-charging a cent. The card can also be topped up with
> stablecoins.**

Only say the last sentence once V3 exists. Only say "credit" once V4 exists.

## 2. Why this use case

V1 is strong infrastructure with no story: "a transfer API" does not tell a reader who uses it.
A card gives it one, and adds the three things V1 does not have:

1. **A deadline.** The card network wants a yes or no in about two seconds. That gives the
   project a real latency budget and a benchmark worth showing.
2. **Money that moves twice.** A tap only holds the money. The real amount arrives days later
   in a clearing file, and it is often different (tips, hotels, partial shipments).
3. **A third party that disagrees with you.** The network's daily file is the truth the
   issuer must reconcile against, which is exactly what V1's reconciliation was built for.

Every fintech that issues cards (Ramp, Brex, Revolut, Marqeta, Lithic) works on these problems.
Crypto card companies (ether.fi, Gnosis Pay, MetaMask card) work on them plus the chain.

## 3. How a card payment works, in plain words

```text
 tap at a shop
   -> card network asks us: "approve $50 at Joe's Cafe?"         (authorization)
   -> we check card, balance, limits; hold $50; answer "yes"      (under the deadline)
 ...one to three days later...
   -> network's daily file: "Joe's Cafe settled $60" (tip added)  (clearing)
   -> we turn the $50 hold into a $60 charge, or release it if it never clears
   -> we pay the network the day's total; reconcile our records against its file
```

In ledger terms, each cardholder has two accounts:

```text
 available  --hold-->  held  --clear-->  network payable  --settle-->  network
     ^                   |
     +------release------+   (reversal, expiry, or clearing for less than the hold)
```

Holds are ordinary balanced ledger transactions, so V1's guarantees (balanced entries,
immutable history, no overspend) cover them for free.

## 4. V2: debit card (the core, no blockchain)

Each phase ends with its failure tests passing, its PR merged, and him explaining it back,
the same as V1. He types one interview-critical piece per phase by hand (marked ✍️).

### Phase 8: a fast, isolated spending path
Measured first in [BENCHMARKS.md](BENCHMARKS.md): one hot account topped out near 390 req/s,
got slower as its history grew, and starved every other account of pool connections.
Authorization (phase 11) is the same path with a deadline, so it was fixed before cards.
- A stored balance per account, maintained by the ledger writer and refused below zero by a
  database CHECK; settlement accounts keep a derived balance.
- Transfer creation claims the idempotency key first and takes the balance lock last.
- Fail fast: a per-account in-flight cap, a lock timeout and a request deadline answer
  `503 account_busy` instead of queueing.
- **Proves:** spending cost does not grow with history, and one busy account cannot take
  the others down.

### Phase 9: cards and card accounts
- Issue a card for an account: card ID, last four digits, status (active, frozen, closed),
  per-transaction and daily limits. No real card numbers, ever (see the scope fence).
- Each cardholder gets an `available` and a `held` ledger account.
- Freeze and unfreeze. A frozen card declines everything.
- **Proves:** the account model that every later phase uses.

### Phase 10: the card network simulator (`mock-network`)
The card network's version of V1's MockBank. It sends what a real network sends:
- authorization requests with a deadline, sometimes duplicated, sometimes retried after a
  timeout;
- reversals, sometimes arriving before the authorization they cancel;
- a daily clearing file, with final amounts that differ from the hold and some transactions
  that were never authorized at all;
- refunds days or weeks later.
- **Proves:** that the backend copes with a messy counterparty, deterministically, in tests.

### Phase 11: authorization ✍️
- Decide yes or no: card active, enough available balance, within limits, merchant category
  not blocked. On yes, move the amount from `available` to `held` in the same transaction.
- **Idempotent on the network's authorization ID:** a retried request gets the same answer and
  never a second hold.
- **Two taps at once cannot overspend:** the balance check and the hold commit together.
  ✍️ He writes this check by hand; it is the question every card interviewer asks.
- If we cannot answer within the deadline, the network decides for us ("stand-in"). Record
  that decision when it arrives and honour it.
- **Proves:** correctness under a deadline. Measured in phase 16.

### Phase 12: reversals, expiry and incremental holds
- A reversal releases the hold exactly once, even if it arrives twice or before the
  authorization.
- A hold that never clears expires after a set number of days and the money is released.
- Hotels and car rentals add to an existing hold (incremental authorization).
- **Proves:** money is never stuck and never released twice.

### Phase 13: clearing
- Read the daily clearing file. Match each line to its hold by the network's IDs.
- Final amount higher than the hold (tips, within a set tolerance): charge the difference.
  Lower (partial shipment): release the rest. No matching hold at all (offline purchase):
  charge it anyway and flag it.
- Re-reading the same file changes nothing.
- **Proves:** the hold-then-settle flow that separates a card backend from a transfer API.

### Phase 14: refunds
- A refund is a new credit back to `available`, never an edit of the original charge.
- Matched to the original purchase when possible; accepted and flagged when not.
- **Proves:** immutable history with money flowing back.

### Phase 15: network settlement and reconciliation
- Each day, total what we owe the network and record the payment.
- Reconcile three ways: our holds and charges, the clearing file, and what we paid.
  V1's six outcome classes extend to card cases (cleared but never authorized, authorized but
  never cleared, amount mismatch, duplicate clearing line).
- Detect and report, never fix automatically, the same rule as V1.
- **Proves:** we can explain every cent to the network and to an auditor.

### Phase 16: benchmark and evidence
- Authorizations per second on a laptop, with p50 and p99 decision time, at fixed rates
  (coordinated-omission safe, the same method as the matching engine).
- Target to measure against, not a promise: p99 under 100 ms. Card processors aim well under
  the network's deadline so retries and network time still fit.
- Chaos runs: duplicate authorizations, reversal before authorization, a process killed
  mid-hold, a clearing file read twice. Each with a named test and a line in the README.
- README rewritten around the card story, with the numbers and the tests.

## 5. V3: stablecoin top-up (the crypto add-on)

Only after V2 is complete. Solves the two problems a bank card never has.

**Problem 1: the chain is too slow for a tap.** So the chain is never touched during a tap.
- The user deposits USDC into a simple contract on a testnet (local Anvil first).
- A watcher credits `available` only after enough confirmations, and undoes the credit if a
  reorganisation removes the deposit.
- Taps then approve against the off-chain balance as in V2.

**Problem 2: two sources of truth.** The user could withdraw on-chain while a hold is open.
- Withdrawals go through a request with a delay. The backend refuses to sign a withdrawal
  that would leave open holds unfunded.

**Then:**
- Settle card spending on-chain in batches; a failed or stuck transaction (gas, nonce) is
  retried without paying twice.
- Reconciliation gains a third record: the chain.
- **Proves:** the hardest part of crypto cards, connecting a two-second card deadline to a
  chain that is slow, can fail and cannot be undone.

## 6. V4: a simple credit limit (later)

- Approve against `limit - used - holds` instead of the balance. Repayments restore the limit.
- No interest, statements, minimum payments or late fees. Those are a separate product.
- Only then may the pitch say "debit and credit".

## 7. Further out, unchanged from PLAN.md

- A real payment provider (sandbox) in place of MockBank.
- FX and multiple currencies (expiring quotes).
- Scalability pass: Redis for rate limits and hot counters, load testing at scale.

## 8. Scope fence

**In:** one currency (USD), virtual debit cards, the simulator, authorization, holds,
reversals, expiry, incremental holds, clearing, refunds, network settlement, reconciliation,
benchmark. V3 and V4 as written above.

**Out, and saying so is part of the answer:**

```text
real card numbers / PCI      real card networks      fraud scoring / ML
chargebacks and disputes     KYC / AML               physical cards, PINs, 3-D Secure
interest and statements      multiple currencies     mobile app / UI
```

Real card numbers are out because storing them brings PCI compliance; production issuers
use a tokenisation vault from a provider. The simulator uses card IDs instead.

## 9. Time, honestly

Roughly 4-6 weeks of evenings and weekends for V2 at about an hour on a workday. V3 is about
two more weeks. If an interview lands mid-way, the finished phases are still a story:
"authorization and holds are done and tested; clearing is next."

## 10. Interview questions each phase prepares

| Question | Phase |
|---|---|
| Two taps at the same moment with $50 in the account. What happens? | 11 |
| The network times out and retries the same authorization. | 11 |
| A reversal arrives before the authorization. | 12 |
| Why does the final amount differ from the hold, and how do you post it? | 13 |
| A charge arrives with no authorization. Do you accept it? | 13 |
| How do you prove to the network that your books are right? | 15 |
| What is your authorization latency, and how did you measure it? | 16 |
| How does a crypto card approve a tap if the chain takes minutes? | V3 |
| What stops a user withdrawing USDC while a hold is open? | V3 |

## Phase log

**2026-10-17 — Phase 8, fast spending path (PR #10, merged).** Stored balance per account
with a `CHECK (balance >= 0)`, idempotency key claimed first and balance lock taken last,
fail-fast admission (`503 account_busy`). Proof: overdraft refused by the database,
`VerifyBalances` 0 drift after every benchmark step. Number: balance read 0.12 ms at 1M
entries; hot account 1,212 accepted/s vs ~330 on same-session main.

**2026-10-17 — Phase 9, cards and card accounts (PR #11, in review).** Cards (status,
limits, cosmetic last4, no card numbers) and one held account per funding account,
created race-free in the statement that wins `card_accounts`. Transitions are single
guarded UPDATEs, each change audited through the outbox. Proof:
`TestConcurrentFirstCardsShareOneHeldAccount`, `TestIssueCardIsIdempotent`,
`TestConcurrentFreezeAndCloseEndClosed`. Number: no regression (spread p99 at 500 6.2 ms vs
main 18.9 ms same session).

**2026-10-17 — Phase 10, card network simulator (PR #12, in review).** `mock-network` and the
`internal/cardnetwork` library, written to `docs/mock-network-api.md`: signed
authorizations with a deadline, one same-`auth_id` retry, stand-in with an advice;
reversals that may precede their authorization; reversals/advices retried with bounded
backoff; ten seeded scenarios; a clearing file built from the network's journal. Proof:
`TestSlowIssuerGetsOneRetryWithTheSameAuthID`, `TestStandInApprovesAfterTwoTimeoutsAndSendsAnAdvice`,
`TestEachScenarioProducesItsMessages`, `TestSameSeedSameBytes`,
`TestClearingFileReflectsTheJournal`. Number: none; no API path changed (regression run
deferred to phase 11).

**2026-10-17 — Phase 11, card authorization (PR #13, in review).** `POST /network/authorizations`
and `POST /network/advices`, signed with `NETWORK_SECRET`, 1 s deadline, answer = the
stored row. The `auth_id` primary key is inserted first; then the outbox event, the daily
counter (one conditional upsert) and the hold last (`available -> held`, balance CHECK).
An overdraft or daily-limit failure rolls the whole transaction back and a second
statement stores the decline, unless a concurrent duplicate already stored an answer.
Advices are deduplicated in `network_messages` and hold `min(amount, available)`, recording
the rest as `hold_shortfall`. Held accounts are refused as transfer endpoints. Proof:
`TestSameAuthIDAlwaysGetsTheSameAnswer`, `TestTwoTapsAtOnceCannotOverspend`,
`TestConcurrentTapsRespectTheDailyLimit`, `TestStandInAdviceIsHonoured`,
`TestNetworkScenariosAgainstTheIssuer`. Number: see BENCHMARKS.md phase 11.

**2026-10-17 — Phase 12, reversals, expiry and incremental holds (PR #14, in review).**
Migration 18 adds `held_amount` / `released_amount` / `expired_at` to `card_authorizations`
(backfilled) and `pending_reversals`. One release primitive, `holds.Release`: row lock,
`min(amount, held_amount)`, daily counter given back, event, ledger `held -> available`
last. `POST /network/reversals` deduplicates on `reversal_id`; a reversal with nothing to
release yet is parked and applied by the approval or advice in the hold's transaction,
serialised with it by a per-`auth_id` advisory lock. `cmd/hold-expirer` expires one hold per
short `SKIP LOCKED` transaction. Incremental authorizations lock the original and hold the
chain total on it. Proof: `TestReversalReleasesTheHoldOnce`,
`TestDifferentReversalsNeverReleaseMoreThanHeld`,
`TestReversalBeforeAuthorizationIsAppliedOnArrival`, `TestReversalRacingExpiryReleasesOnce`,
`TestExpiryReleasesOldHoldsOnce`, `TestConcurrentIncrementalsRespectFundsAndLimits`. Number:
see BENCHMARKS.md phase 12.

**2026-10-17 — Phase 13, clearing (PR #15, in review).** Migration 19: `clearing_files` (raw bytes,
SHA-256 unique), `clearing_records` (`clearing_id` primary key, status, charged from held /
available, released, shortfall, flag, ledger link), `cleared_amount` / `cleared_at` on
`card_authorizations`, and the `acc_network_payable_usd` settlement account.
`clearing.Import` validates the whole file first, stores it once by hash, then posts each
line in its own transaction: record inserted first (`ON CONFLICT DO NOTHING`), `auth_id`
advisory lock and authorization row in the same round trip, balances locked in id order
only when the available account pays, one balanced ledger transaction last. Exact,
partial (rest released, daily counter freed) and tip lines; over-tolerance, after-expiry,
after-reversal, duplicate, unauthorized, unknown-card and shortfall lines flagged; refunds
deferred to phase 14. `cmd/clearing` imports from disk or `GET /clearing?date=`, lists and
inspects. Proof: `TestClearingSameFileTwiceChangesNothing`,
`TestClearingConcurrentImportsOfOneFile`, `TestClearingCrashMidFileRerunPostsEachLineOnce`,
`TestClearingRacingReversalAndExpiry`, `TestShortfallNeverOverdraws`,
`TestNetworkScenariosClearEndToEnd`. Number: see BENCHMARKS.md phase 13.
