# Design a payment platform

> Design a service that lets a business move money from one account to another through an
> external provider, and never loses, duplicates or invents a cent — under retries,
> concurrency, duplicate provider events, provider failures and provider disagreement.

Answered in the standard order. Deep dives are added as each phase is built.

---

## 1. Functional requirements

**In scope**

1. Create an account.
2. Move money between two accounts — source debited, destination credited.
3. Read a transfer's status and history.
4. Read an account's balance.
5. Receive provider settlement events and advance the transfer.
6. Reconcile our records against the provider's daily file and classify every difference.

**Out of scope** — say this out loud, it is scoping, not ignorance

```
cards · FX / multi-currency · fraud · KYC/AML · refunds · payouts to platforms · multi-tenancy
```

---

## 2. Non-functional requirements

Ordered. The order is the answer — a payments interviewer is testing whether correctness is
ranked above availability, and it is.

1. **Correctness over availability.** Refuse the transfer rather than risk moving money twice.
   The ledger must balance at all times; no partial writes.
2. **Idempotent.** A retried request creates one transfer and returns the same answer.
3. **Durable + auditable.** Every financial fact is permanent and append-only. No update or
   delete on a ledger entry, ever.
4. **Resilient to a hostile provider** — timeouts after success, duplicate events, out-of-order
   events, silence.
5. **Eventually consistent with the provider,** and the disagreements are *detected*, not
   silently fixed.
6. Scale: ~100 transfers/sec, p99 < 200ms on the write path. Small — and that is a deliberate
   claim, not an omission.

**Explicitly not a goal:** synchronously telling the caller the money has arrived. That is a
promise no system with an external rail can keep.

---

## 3. Core entities

```
Account          who holds money                   id · currency · type
LedgerTxn        one balanced movement             id · reference · created_at
LedgerEntry      one side of a movement  ── APPEND ONLY ──
                 txn_id · account_id · direction(debit|credit) · amount · created_at
Transfer         the business intent + state       id · src · dst · amount · status · provider_ref
```

**The distinction that gets asked about:** `Transfer` is the *intent* ("pay this vendor $500");
`LedgerTxn` is the *accounting fact*. One transfer may produce several ledger transactions over
its life — the transfer, then a reversal, then a fee. Collapsing them into one table is the
common wrong answer.

```
Transfer 1 ──< LedgerTxn 1 ──< LedgerEntry (exactly 2 per txn in V1, always balanced)
```

**Money is an integer in minor units.** `$10.50` → `1050`. Never a float: `0.1 + 0.2 != 0.3` in
binary floating point, and those errors accumulate into real missing money.

**There is no `balance` column.** A balance is derived:

```
balance(account) = Σ credits − Σ debits          ← computed from entries, never stored
```

A stored balance is a number that can be wrong. A derived balance cannot disagree with the
entries, because it *is* the entries.

---

## 4. API

```
POST /v1/accounts                        → 201 { id, currency, type }
GET  /v1/accounts/{id}                   → 200 { id, balance, currency }   balance is derived

POST /v1/transfers                       → 202 { id, status: "processing" }
     Authorization: Bearer <api_key>
     Idempotency-Key: <client uuid>      ← client-supplied; makes THEIR retry safe
     { source_account, destination_account, amount: 50000, currency: "USD" }

GET  /v1/transfers/{id}                  → 200 { id, status, amount, timeline[] }
GET  /v1/transfers                       → 200 { data[], next_cursor }

POST /v1/webhooks/provider               ← provider calls US; signed, verified, idempotent
```

Two deliberate choices to defend:

- **`202`, not `201`** — we accepted the instruction; the money has not moved.
- **`status: "processing"`, never `settled` at creation** — see §5.

---

## 5. High-level design

```
 ┌────────┐   POST /v1/transfers                  ┌──────────────────────┐
 │ Client │──── Idempotency-Key ─────────────────▶│  API                 │
 └────────┘                                       │  authn → rate limit  │
      ▲                                           │  → validate          │
      │        202 {status:"processing"}          └──────────┬───────────┘
      └──────── money has NOT moved yet ─────────────────────┤
                                                             │
        ┌────────────────────────────────────────────────────▼─────────────┐
        │  ONE DB TRANSACTION — all of it commits, or none of it does      │
        │                                                                  │
        │   INSERT transfer                        status = processing     │
        │   INSERT ledger_txn                                              │
        │   INSERT entry  debit   source   50000                           │
        │   INSERT entry  credit  settlement 50000   ◀── NOT the vendor:   │
        │                                                money is with us  │
        │   INSERT outbox_event                    ◀── same DB, same txn.  │
        │                                              This is why no      │
        │   refuse commit unless Σdebits == Σcredits    event is ever lost  │
        └───────────────────────────┬──────────────────────────────────────┘
                                    │
                              ┌─────▼──────┐
                              │ PostgreSQL │  source of truth
                              └─────┬──────┘
                                    │ relay polls unpublished rows
                                    ▼
                              ┌──────────┐
                              │  Kafka   │  transport only — the OUTBOX is the guarantee
                              └─────┬────┘
                                    │ at-least-once → consumers MUST be idempotent
                                    ▼
                           ┌─────────────────┐   "please move $500"   ┌──────────┐
                           │ payment worker  │──────────────────────▶ │ Provider │
                           └─────────────────┘                        └────┬─────┘
                                                                           │
                    ┌──────────────────────────────────────────────────────┘
                    │  webhook, minutes later, maybe twice, maybe out of order
                    ▼
           ┌──────────────────┐        settlement credited, transfer → SETTLED
           │ webhook handler  │───────▶ (a SECOND ledger txn — the first is never edited)
           │ verify · dedupe  │
           └──────────────────┘
                    ┌─────────────────────────────────────────┐
     nightly ──────▶│ reconciler: our records vs their file    │──▶ exceptions for a human
                    │ MATCHED · AMOUNT · MISSING · STATUS · DUP│
                    └─────────────────────────────────────────┘
```

### The one idea the whole diagram exists to serve

```
   commit in our DB   ─────────────────  money actually moved at the provider
        (instant)          THE GAP            (seconds to days later, maybe never)
```

Everything hard in this system lives in that gap. The outbox, the workers, the webhooks and
reconciliation are all consequences of it. Answering "why is this not just an UPDATE?" with
that one line is the whole interview.

### Why the money is credited to a settlement account, not the vendor

At commit time the vendor has nothing. The money is ours, earmarked. Crediting the vendor
immediately would record a fact that is not true — and the ledger is append-only, so that lie
would be permanent. The vendor is credited by a *second* transaction when the provider
confirms.

### Why the outbox and not just publishing to Kafka

```
   INSERT transfer   ✓ committed
   publish to Kafka  ✗ process dies here      ← transfer exists, nobody will ever process it
```

Two separate systems cannot commit atomically, and no amount of ordering or care fixes it.
Writing the event into the *same* database transaction makes it impossible to have one without
the other. Kafka is then just transport — and that is the honest answer to "why Kafka?", not
"it scales".

---

## 6. Deep dives

Added as each is built, so they are derived rather than memorised.

```
[ ] Concurrent overspend      two $700 transfers, $1,000 account — exactly one wins
[ ] Idempotency under load    100 identical requests → 1 transfer, 99 identical replies
[ ] Provider timeout after success   we do not know if it moved. Retry = double pay.
                                     Fail = lost pay. It stays UNRESOLVED.
[ ] Duplicate / out-of-order webhooks
[ ] At-least-once redelivery — worker killed mid-processing
[ ] Reconciliation classification
[ ] Rate limiting across instances
```
