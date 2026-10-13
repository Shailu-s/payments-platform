# Roadmap after V1: a debit card backend

**Written:** 2026-10-13. **Builds on:** V1 in [PLAN.md](PLAN.md) (ledger, idempotency,
overspend protection, outbox + Kafka, signed webhooks, reconciliation). Nothing in V1 is thrown
away; every new piece sits on top of it.

This file replaces the "Later versions" list at the end of PLAN.md's scope fence. The working
method, the rules and the session LOG stay in PLAN.md.

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

### Phase 8: cards and card accounts
- Issue a card for an account: card ID, last four digits, status (active, frozen, closed),
  per-transaction and daily limits. No real card numbers, ever (see the scope fence).
- Each cardholder gets an `available` and a `held` ledger account.
- Freeze and unfreeze. A frozen card declines everything.
- **Proves:** the account model that every later phase uses.

### Phase 9: the card network simulator (`mock-network`)
The card network's version of V1's MockBank. It sends what a real network sends:
- authorization requests with a deadline, sometimes duplicated, sometimes retried after a
  timeout;
- reversals, sometimes arriving before the authorization they cancel;
- a daily clearing file, with final amounts that differ from the hold and some transactions
  that were never authorized at all;
- refunds days or weeks later.
- **Proves:** that the backend copes with a messy counterparty, deterministically, in tests.

### Phase 10: authorization ✍️
- Decide yes or no: card active, enough available balance, within limits, merchant category
  not blocked. On yes, move the amount from `available` to `held` in the same transaction.
- **Idempotent on the network's authorization ID:** a retried request gets the same answer and
  never a second hold.
- **Two taps at once cannot overspend:** the balance check and the hold commit together.
  ✍️ He writes this check by hand; it is the question every card interviewer asks.
- If we cannot answer within the deadline, the network decides for us ("stand-in"). Record
  that decision when it arrives and honour it.
- **Proves:** correctness under a deadline. Measured in phase 15.

### Phase 11: reversals, expiry and incremental holds
- A reversal releases the hold exactly once, even if it arrives twice or before the
  authorization.
- A hold that never clears expires after a set number of days and the money is released.
- Hotels and car rentals add to an existing hold (incremental authorization).
- **Proves:** money is never stuck and never released twice.

### Phase 12: clearing
- Read the daily clearing file. Match each line to its hold by the network's IDs.
- Final amount higher than the hold (tips, within a set tolerance): charge the difference.
  Lower (partial shipment): release the rest. No matching hold at all (offline purchase):
  charge it anyway and flag it.
- Re-reading the same file changes nothing.
- **Proves:** the hold-then-settle flow that separates a card backend from a transfer API.

### Phase 13: refunds
- A refund is a new credit back to `available`, never an edit of the original charge.
- Matched to the original purchase when possible; accepted and flagged when not.
- **Proves:** immutable history with money flowing back.

### Phase 14: network settlement and reconciliation
- Each day, total what we owe the network and record the payment.
- Reconcile three ways: our holds and charges, the clearing file, and what we paid.
  V1's six outcome classes extend to card cases (cleared but never authorized, authorized but
  never cleared, amount mismatch, duplicate clearing line).
- Detect and report, never fix automatically, the same rule as V1.
- **Proves:** we can explain every cent to the network and to an auditor.

### Phase 15: benchmark and evidence
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
| Two taps at the same moment with $50 in the account. What happens? | 10 |
| The network times out and retries the same authorization. | 10 |
| A reversal arrives before the authorization. | 11 |
| Why does the final amount differ from the hold, and how do you post it? | 12 |
| A charge arrives with no authorization. Do you accept it? | 12 |
| How do you prove to the network that your books are right? | 14 |
| What is your authorization latency, and how did you measure it? | 15 |
| How does a crypto card approve a tap if the chain takes minutes? | V3 |
| What stops a user withdrawing USDC while a hold is open? | V3 |
