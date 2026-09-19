# Payments Platform — what we are building and how

**Started:** 2026-09-19
**Language:** Go. **Database:** PostgreSQL. **Events:** Kafka. **Local:** Docker Compose.
**Repo:** standalone, public, GitHub identity `Shailu-s` (set `user.name`/`user.email`
**locally** in this repo — the global config is the Qiro work identity and must never land
on a portfolio commit).

---

## 🚨 Working method (non-negotiable)

**Claude instructs. Shailendra types.**

- Claude explains what to build and why, then he writes the code.
- If a snippet is genuinely needed, it goes in chat to copy — not written into project files.
- Reviewing and correcting his code afterwards is Claude's job.
- **Why:** code he did not type teaches him nothing, and interviewers ask about every line.

Update the LOG at the bottom of this file at the end of every session.

---

## 1. The one-sentence answer

When someone asks what this is:

> **I built payment infrastructure for reliably moving money through external providers. It
> maintains its own double-entry ledger and stays correct under retries, concurrency,
> duplicate events, provider failures, and reconciliation mismatches.**

Memorise that sentence. It is the pitch, and it works on an engineer and a non-engineer.

## 2. What it is, in plain language

Businesses need to move money. When they do, they use an outside provider — a bank, a card
network, a payment processor. Those providers are **slow, unreliable, and inconsistent**.
They time out after already succeeding. They send the same notification twice. They tell you
a payment settled before they told you it started. Sometimes they just disagree with you
about what happened.

This platform sits in between. An application calls one simple API to move money, and this
system deals with all of that mess while keeping a perfect, permanent record of every cent.

The product is **the infrastructure itself**. Not an app for consumers — a layer other
developers build on. Stripe, Adyen and Modern Treasury sell this; this is a small, honest
version of the part that matters.

## 3. What a user actually does

They send one request:

```http
POST /v1/transfers
Idempotency-Key: 7da2f1c9-...

{
  "source_account": "acc_company",
  "destination_account": "acc_vendor",
  "amount": 50000,
  "currency": "USD"
}
```

They get one response:

```json
{ "id": "tr_123", "status": "processing", "amount": 50000, "currency": "USD" }
```

`50000` is **cents**, not dollars. That is deliberate and section 5 explains why.

Behind that single call, the system: validates it, checks whether it has seen this
idempotency key before, checks the account has the money, writes the transfer, writes the
ledger entries, and writes an outbox event — **all in one database transaction**. A separate
worker then picks the event up and actually talks to the provider.

The response says `processing`, not `settled`, because the money has not moved yet. Being
honest about that is the whole design.

## 4. The two ideas everything rests on

### Idea 1 — double-entry bookkeeping

Money is never "subtracted from a balance". Every movement is recorded twice: once as where
it came **from**, once as where it went **to**. A $500 transfer:

```
Company account     debit    500.00
Settlement account  credit   500.00
                    -----------------
                    difference: 0
```

Every transaction must satisfy `sum(debits) == sum(credits)`. If that is ever false, the
system has lost or invented money, and we want to know instantly rather than at year end.

A balance is not a number we store and update. It is **derived** by adding up that account's
entries. There is no field to corrupt.

### Idea 2 — the ledger is immutable

Ledger entries are written once and **never updated or deleted**. No `UPDATE ledger_entries`
anywhere in the codebase, ever.

If something is wrong, we write a *new* transaction that corrects it — the way real
accounting works. This means the full history of every cent is permanently auditable, and a
bug can never quietly rewrite the past.

## 5. Money is integers, never floats

`$10.50` is stored as `1050`. Never `10.50` in a float.

Floating point cannot represent most decimal fractions exactly. `0.1 + 0.2` is not `0.3` in
binary floating point. Over millions of operations those errors accumulate into real missing
money, and financial systems do not get to be approximately right.

**We will prove this with a failing test** rather than take it on faith.

## 6. The eight guarantees — this is the centrepiece

The README will open with these, each backed by a named automated test. They are the product.

1. **The ledger always balances.** Every transaction's debits equal its credits.
2. **The same idempotency key never creates two transfers.**
3. **Money cannot be overspent by concurrent requests.**
4. **Duplicate provider events never duplicate financial effects.**
5. **Every financial state change has an audit trail.**
6. **Ledger entries are never modified or deleted.**
7. **Kafka redelivery never causes duplicate processing.**
8. **Provider discrepancies are always detected by reconciliation.**

This is the difference between "93% test coverage" (which says nothing) and eight specific
promises with the proof attached. `shortn` opened with measured k6 throughput numbers; this
one opens with correctness guarantees. Same move, different axis.

---

# How we build it — seven phases

**Every phase ends with a system that works.** No phase leaves things half-finished, so the
project can never strand us the way a five-weekend plan with nothing runnable does.

## Phase 1 — the ledger

No API, no Kafka, no providers. Just money, correctly modelled.

Three tables:

```
accounts              ledger_transactions        ledger_entries
--------              -------------------        --------------
id                    id                         id
currency              reference                  transaction_id
type                  created_at                 account_id
created_at                                       direction  (debit|credit)
                                                 amount     (integer minor units)
                                                 created_at
```

Write a function that records a transaction with its entries, in one DB transaction, and
refuses to commit if the debits and credits do not match. Write a function that derives an
account balance from its entries.

**Ends with:** a test that moves money between two accounts and asserts the books balance,
plus the failing float test from section 5.

**Teaches:** financial modelling, DB transactions, invariants, immutability.

## Phase 2 — the transfer API

Now it becomes a product other people can call.

```
POST /accounts      GET /accounts/:id
POST /transfers     GET /transfers/:id      GET /transfers
```

A transfer is a **business-level object** (who paid whom, how much, what state, which
provider) that sits *on top of* the ledger. The ledger records the accounting fact; the
transfer records the real-world intent. Keeping them separate matters, because one transfer
may produce several ledger transactions over its life.

Lifecycle for now:

```
CREATED → PROCESSING → SETTLED
                    ↘  FAILED
```

**Ends with:** `curl POST /transfers` works end to end.

## Phase 3 — idempotency and concurrency

This is where it becomes a serious backend, and it covers a known gap of his.

**Idempotency.** The client's network flakes and it retries the same request ten times. The
system must create **one** transfer and return the same answer ten times — not an error, the
*same result*.

The wrong implementation is `SELECT`, then `INSERT if not found`. Two requests arriving at
the same instant both see "not found" and both insert. **Only a database UNIQUE constraint
actually serialises them.**

Test to write: 100 simultaneous requests, same key → 1 transfer created, 99 get the same
response.

**Balance concurrency.** An account holds $1,000. Two $700 transfers arrive at the same
moment. Exactly one must succeed.

We will implement this **two ways** — row locks (`SELECT ... FOR UPDATE`) and
`SERIALIZABLE` with retry on serialization failure — measure both, and pick one **with a
stated reason**. Having actually measured it is the difference between a senior answer and a
staff answer.

**Ends with:** two concurrency tests that fail first, then pass.

## Phase 4 — the provider simulator (MockBank)

Introduce the outside world, on our terms.

MockBank is a separate small service that behaves like a real bank rail: accepts a transfer,
returns a provider reference and `processing`, then **later, asynchronously, sends a webhook**
saying it settled.

```
API → Postgres → worker → MockBank
                              │
                              └── webhook ──→ our webhook handler
```

Building the provider ourselves is a deliberate choice: we get to control exactly how it
misbehaves, which is what Chaos Mode is built on. One provider is enough for V1.

**Ends with:** a transfer that goes `PROCESSING` and later becomes `SETTLED` on its own.

## Phase 5 — Kafka and the transactional outbox

**The hardest and most valuable part of the project.**

The problem: a transfer is created and several things must follow — process the payment,
notify the customer, update reconciliation. We cannot do this:

```
1. INSERT transfer into Postgres   ✓ committed
2. Publish event to Kafka          ✗ process crashes here
```

The database now says the transfer exists and nobody will ever process it. The money is
stuck. This is a **dual-write** problem, and it cannot be solved by reordering the two steps
or by being careful — there is no way to make two separate systems commit atomically.

The fix — the **transactional outbox**:

```
BEGIN
  INSERT transfer
  INSERT outbox_event      ← same database, same transaction
COMMIT
```

Either both land or neither does. A separate **relay** worker then reads unpublished outbox
rows and publishes them to Kafka. If it crashes, it retries and may publish twice — which is
fine, because consumers are idempotent (that is guarantee #7, and phase 3 built it).

Then the consumers: payment processor, webhook sender, reconciliation feeder. **He writes a
Kafka consumer himself** — subscribe, partition assignment, read loop, offset commit — and
then we kill a worker mid-processing so at-least-once redelivery stops being theory.

> **Note for interviews, and be honest about it:** at V1 volume, Postgres alone could carry
> this. The **outbox** is what provides the guarantee; Kafka is just the transport. Kafka is
> here partly as a learning objective. The strong answer to "why Kafka?" is exactly that
> sentence — not "it's scalable".

**Ends with:** a transfer surviving a worker killed mid-flight.

## Phase 6 — webhooks, both directions

Two separate systems, two different hard problems.

**Inbound** (provider → us). Must handle: signature verification, duplicate events, events
arriving **out of order** (a `settled` webhook before the `created` one), replay attacks,
malformed payloads, and events referencing transfers we have never heard of. Never trust the
order, and never trust that you have seen a message only once.

**Outbound** (us → our customers). Their endpoint will be down, slow, or returning 500s.
Needs: retries with exponential backoff, a delivery history, request signing so they can
verify it came from us, a dead-letter queue for permanent failures, and manual replay.

**Ends with:** a customer endpoint that fails 5 times then succeeds, and the DLQ catching one
that never does.

## Phase 7 — reconciliation

The most fintech-specific problem, and the one that makes this infrastructure rather than an
API.

Our records and the provider's records **will** disagree. Not occasionally — routinely. A
nightly job pulls the provider's settlement file, compares it to internal truth, and
classifies every difference:

```
MATCHED              both agree
AMOUNT_MISMATCH      we say $500, they say $490
MISSING_EXTERNAL     we have it, they do not
MISSING_INTERNAL     they have it, we do not
STATUS_MISMATCH      we say settled, they say failed
DUPLICATE_EXTERNAL   they have it twice
```

Output:

```
Today's reconciliation — 12,481 transfers checked
  12,473 matched
       3 amount mismatch
       2 missing external
       3 status mismatch
```

Each exception is a row an operator can open and investigate. **Detecting** the break is the
job; automatically "fixing" money is not.

**Ends with:** a deliberately corrupted provider file, fully classified.

---

# Then: Chaos Mode

This is what makes the project memorable, and it turns every correctness claim from an
assertion into a demonstration.

MockBank gets a scenario API:

```http
POST /sandbox/scenarios
{ "transfer_id": "tr_123", "scenario": "timeout_after_success" }
```

Scenarios:

```
duplicate_webhook          settle_before_ack         wrong_amount
delay_webhook              timeout_before_processing failed_transfer
drop_webhook               timeout_after_success     reverse_after_settlement
```

The nastiest is `timeout_after_success`: the provider **did** move the money but our call
timed out, so we genuinely do not know. We must not retry blindly (double payment) and must
not fail it (lost payment). It stays unresolved until the webhook or reconciliation tells us
the truth.

The README then shows real sequences:

```
Inject duplicate webhook          Provider timeout after success
  → received twice                  → transfer left UNRESOLVED
  → processed once                  → webhook arrives later
  → ledger unchanged ✓              → transfer SETTLED
                                    → reconciliation MATCHED ✓
```

This also has genuine open-source value on its own — anyone building a payment integration
needs a provider that can be told to misbehave.

---

# The dashboard

Deliberately minimal. This is not a frontend showcase; it exists so the engineering is
**visible** and screenshottable. Four screens:

1. **Transfers** — list with live status.
2. **Transfer detail** — the full timeline: created → processing → provider accepted →
   webhook received → settled. This screen is the audit trail made visible.
3. **Reconciliation** — today's match counts, click a mismatch for the detail.
4. **Chaos** — pick a transfer, pick a failure, inject it, watch the system absorb it.

Screen 4 is the one that gets recorded as a GIF for the top of the README.

---

# Structure

A **modular monolith plus workers** — not microservices. Microservices here would add
deployment complexity and distributed failure modes while teaching nothing extra, and
"I chose a modular monolith because the coupling did not justify the operational cost" is a
better interview answer than an unjustified service mesh.

```
/apps
    /api          HTTP API
    /worker       outbox relay, payment processor, webhook sender, reconciler
    /mock-bank    the fake provider + chaos scenarios
/internal
    /accounts  /ledger  /transfers  /providers
    /webhooks  /reconciliation  /outbox  /events
/db             migrations
/docker         compose
```

---

# Scope fence for V1

**In:** one currency (USD), one rail (MockBank), accounts, transfers, double-entry ledger,
idempotency, concurrency, Kafka, outbox, webhooks in and out, retries, DLQ, reconciliation,
chaos scenarios, a `/metrics` endpoint with a few counters, the four dashboard screens.

**Out — and this list is the reason the project will actually finish:**

```
cards          crypto         FX / multi-currency      real bank integration
Kubernetes     fraud          KYC / AML                multi-region
Redis          OpenTelemetry tracing                   Grafana dashboards
microservices
```

Redis is out because nothing in V1 needs it and `shortn` already covers Redis. Tracing is out
until there is a latency question worth answering.

Later versions, strictly after V1 is complete and only if wanted:

```
V2  a real payment provider
V3  FX + multi-currency (expiring quotes, the race when a quote dies mid-conversion)
V4  a stablecoin rail  ← the differentiator: settlement that is slow AND probabilistic
                          (confirmations, reorgs, gas failure). Mentioned in interviews,
                          never the pitch.
V5  load testing at scale
```

**Crypto is deliberately V4 and deliberately quiet.** It keeps the project domain-neutral —
"payment infrastructure" reads as fintech to every company, while "crypto card" only reads
to crypto companies. The stablecoin rail then becomes the one differentiating section rather
than the identity of the project.

---

# Rules that keep this alive

- **Weekend track.** This must not eat the weekday retrieval/SD slot.
- **Every phase runs before the next begins.** No exceptions — this is what `card-engine`
  got wrong.
- **Write the failing test first** for every one of the eight guarantees. Watch it fail.
- **Small, reviewable commits** following the build order. No AI attribution trailers, ever.
  After the first commit, stop and let him review before continuing.
- **Never commit secrets or generated files.** `git status --porcelain -uall` before staging.
- If a phase is dragging past its weekend, **cut scope within the phase** rather than
  skipping ahead.

---

# LOG

*(Update at the end of every session: what was built, what broke, what he could not explain.)*

**2026-09-19 — plan written. Nothing built yet.**
Decisions made in the design conversation:
- Rejected: expense trackers, CRUD banking apps, and the narrow "spend from yield" card
  product (too small to be a system, and crypto-first narrows the audience).
- Rejected: framing this as a pure capability ("a ledger with an invariant") — no product,
  no screen, nothing a non-engineer can follow.
- Chose: rail-agnostic payment infrastructure, one rail in V1, crypto as V4.
- Chose: Chaos Mode folded in as a feature of the provider simulator rather than a separate
  project, because it makes the correctness claims demonstrable.
- Chose: the eight guarantees as the README centrepiece, mirroring `shortn`'s
  measured-numbers opening.
- Kafka kept, but for the honest reason (a named learning gap + the outbox story), not
  because V1 volume needs it.
- Standalone repo outside `interview-helper`, identity `Shailu-s` set locally.

**Next session — Phase 1:** the three tables, the balanced-transaction writer, the derived
balance, and the two tests (books balance; float loses money).
