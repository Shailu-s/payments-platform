# System design: a debit card backend

Written like an interview answer: requirements, numbers, the simple design, then how to
scale it one step at a time, and only when a measured number demands it.

## 1. What we are building

A backend that moves money reliably through outside providers (V1), then becomes a
**debit card backend**: a card tap gets yes/no fast, money is held, and the final amount
settles days later (V2). Later: stablecoin top-up (V3), a simple credit limit (V4).

## 2. Requirements

**Functional**
- Accounts, transfers through a bank provider, double-entry ledger (V1, done).
- Issue/freeze cards; authorize a tap; hold money; reversals and expiry; clearing file;
  refunds; daily settlement and reconciliation with the network (V2).
- Deposit USDC on-chain, credit after confirmations; safe withdrawals (V3).

**Non-functional, in priority order**
1. **Correct money.** Never lose, invent or double-spend a cent, under retries, crashes,
   duplicates and concurrency.
2. **Fast authorization.** p99 < 100 ms (the card network gives us ~2 s in total).
3. **Available.** One busy account must not slow down everyone else.
4. **Auditable.** Immutable history; every cent explainable to an auditor.

## 3. Back-of-envelope

Target: **10 million requests/day** on one laptop.

```text
10M / 86,400 s        ~ 115 req/s average
peak ~ 5x average     ~ 600 req/s
each payment          ~ 1 transfer row + 2-3 ledger entries + 1 outbox row
storage               ~ 10M x ~1 KB ~ 10 GB/day  -> growth matters before speed does
```

Measured today (V1, laptop, one Postgres, 10 DB connections):

```text
spread across accounts   ~ 1,100 req/s   (~ 95M/day if flat)
one hot account          ~   400 req/s   (and it slows down the others)
```

**So V1 already handles "millions per day".** The real risks are one hot account and
data growth, not average traffic.

## 4. Core entities

`Account`, `LedgerTransaction`, `LedgerEntry` (immutable), `Transfer`, `OutboxEvent`,
`ApiKey`, `Card`, `Authorization` / `Hold`, `ClearingRecord`, `ReconciliationRun`.

## 5. API (main calls)

```text
POST /v1/transfers                 Idempotency-Key header         (V1)
POST /v1/cards        POST /v1/cards/{id}/freeze                  (V2)
POST /network/authorizations       idempotent on network auth id  (V2, the hot path)
POST /network/reversals            clearing file import (batch)   (V2)
```

## 6. The simple design (build this first)

```text
client / card network
        |
      API (one Go process)
        |  one Postgres transaction: business row + ledger entries + outbox row
     Postgres  (the only source of truth for money)
        |
     relay -> Kafka -> workers (send to bank, notify customer)
        |
     provider / network simulators  <- webhooks / clearing files
        |
     reconciler (daily, detect only, never auto-fix)
```

| Part | Simple version | Why it is enough for now |
|---|---|---|
| API | 1 process | stateless; ~1,100 req/s measured |
| Database | 1 Postgres | money needs one source of truth; ACID gives correctness for free |
| Idempotency | UNIQUE index on the key | the database, not code, picks one winner |
| Overspend | lock the account row, then check | correct; slow only for one very busy account |
| Events | outbox + relay + Kafka | no lost events on crash; Kafka is transport, not the guarantee |
| Card auth (V2) | same transaction shape: check, move available -> held | same guarantees as transfers |
| Clearing | read the daily file in a batch job | once a day; no latency need |
| Reconciliation | batch job, detect only | correctness, not speed |

## 7. How to scale it, step by step

Do a step **only when its trigger is measured**. Each step has a cost.

| # | Trigger (measured) | Change | And the cost is |
|---|---|---|---|
| 1 | one hot account slows everyone | fail fast: per-account limit, lock timeout, request deadline | some payments get a fast 503 and must retry |
| 2 | balance check gets slower with history | keep the balance in one row, DB `CHECK (balance >= 0)` | a stored number that must never drift from the ledger |
| 3 | every request spends DB trips on auth and rate limits | cache API keys; rate limit in Redis | key revocation delay; one more component |
| 4 | one API process is CPU-bound | more API processes + a connection pooler (PgBouncer) | ops; the DB becomes the limit |
| 5 | tables too big, reads slow down writes | partition ledger by month; read replica for GETs and reconciliation | partition upkeep; replica lag on reads |
| 6 | one account needs > ~1,000/s | batch its payments into one transaction, or split it into sub-balances | added latency per batch, or more complex balance logic |
| 7 | one Postgres is not enough | shard accounts across databases, or a ledger database (TigerBeetle) | cross-shard transfers get hard; new operational model |

## 8. Should we scale now?

**No. Keep building the simple card backend.** The numbers say V1 already covers
millions of requests a day. Scaling now adds complexity that interviewers will ask us to
justify, and we could not.

Two rules instead:
- **Every phase runs the benchmark** (`make bench-http`) and records the number.
- **Scale only when a requirement breaks**: card auth p99 > 100 ms, or one account
  slowing others. Then apply the next step from the table, no more.

Open decision: PR #10 bundles steps 1 and 2. Step 1 protects other accounts; step 2 is
only needed when card authorization shows the need. Decide whether to keep both, keep only
step 1, or park the PR until the card phase measures it.
