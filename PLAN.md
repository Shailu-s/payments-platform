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

## 7. The evidence rule — no claim without a number or a failure

**Every architectural word in the README must be there because it was measured or because a
failure case forced it.** Not because it is what payment systems have.

If the README says a thing, one of these must be true and visible:

```
"we use SELECT FOR UPDATE"    → the benchmark against SERIALIZABLE, and why this one won
"the outbox guarantees X"      → the killed-worker test that would lose the event without it
"idempotent under concurrency" → the 100-goroutine test, and the double-spend it does without
"Kafka"                        → the honest answer: a learning gap + the outbox story,
                                 explicitly NOT "it scales"
```

This is the single strongest interview differentiator in the project, because almost nobody
does it. The failure mode it prevents is the one he has: naming a mechanism he cannot justify.

**Mechanically:** every phase below that makes a claim ends with the number or the failed test
that earns it, produced *in that phase* while the context is fresh. There is no separate
"benchmarking phase" — a phase that has not produced its evidence is not finished.

**And the cost is:** every phase is ~20% longer, and some measurements will be boring or will
contradict the assumption. Record the boring ones anyway; "I measured it and the difference was
noise, so I picked the simpler one" is a senior answer.

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

### Authentication — API keys, and deliberately no tenancy

**The constraint:** the moment this is a product other people call, an unauthenticated
`POST /transfers` moves money for anyone who can reach the port. Authentication is not a
feature here, it is the precondition for the API existing at all.

```
api_keys
--------
id
key_hash      ← the hash, never the key
prefix        ← first 8 chars, shown in logs and the dashboard so a key is identifiable
name
created_at
revoked_at
```

The key is shown **once** at creation and never again, because we only store its hash. The
`prefix` column exists so a key can be identified in a log line or revoked from the dashboard
without ever storing the secret. Middleware resolves `Authorization: Bearer <key>` to a key
row, 401s on missing/unknown/revoked.

**Explicitly out: multi-tenancy.** No `tenant_id` on accounts, transfers or ledger entries. A
valid key can act on any account. This is a real limitation and the interview answer is the
honest one:

> "V1 has authentication but not authorisation. Adding tenancy means a `tenant_id` on every
> table and a scoping check on every read path, and I chose to spend that weekend on
> correctness under failure instead. Here is exactly where it would go."

Knowing precisely what is missing and why beats having quietly not thought about it.

### Rate limiting — Postgres-backed, correct across instances

**The constraint:** an in-memory counter is wrong the instant there are two API instances —
two processes each allow the full quota, so the limit is silently double. The limit has to
live in shared state, and the only shared state in V1 is Postgres.

Sliding window or token bucket in a table, keyed by API key, incremented **atomically** —
this is the same race as phase 3's idempotency wearing a different hat, and it should be
recognised as such rather than solved from scratch. Over the limit returns `429` with
`Retry-After`.

**Evidence required (section 7):** a concurrent test that fires N+20 requests against a limit
of N from multiple goroutines and asserts exactly N pass — which fails on the naive
read-then-write implementation. Plus the measured per-request latency cost of the DB round
trip, because that is the real objection to this design.

**And the cost is:** a database write on every single request, on the hot path. Redis exists
precisely to avoid that. The defence is that V1 volume does not care and it avoids a container
for one feature — not that it is better.

**Ends with:** `curl POST /transfers` works end to end with a key, 401s without one, and 429s
when hammered.

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

**The credibility gap — have this answer ready.** We wrote both the client and the provider,
so we have never fought a real integration: no undocumented behaviour, no vendor support
ticket, no rate limit we did not design ourselves. An interviewer will notice. The answer is
not to pretend it is equivalent:

> "I simulated the provider so I could control the failure modes, which means I have tested
> against failures a real sandbox will not produce on demand — and *not* against the failures
> I did not think of. That is the trade, and a real rail is V2."

To narrow the gap cheaply, MockBank should be built to a **written API contract document**
first (endpoints, error codes, webhook payloads, retry semantics) and the client coded against
that document rather than against the implementation — so at least the integration is against
a spec, not against shared memory of how it works.

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
idempotency, concurrency, **API-key authentication**, **Postgres-backed rate limiting**,
Kafka, outbox, webhooks in and out, retries, DLQ, reconciliation, chaos scenarios, a
`/metrics` endpoint with a few counters, the four dashboard screens, **and the measured
evidence behind every claim (section 7)**.

**Out — and this list is the reason the project will actually finish:**

```
cards          crypto         FX / multi-currency      real bank integration
Kubernetes     fraud          KYC / AML                multi-region
Redis          OpenTelemetry tracing                   Grafana dashboards
microservices  multi-tenancy / authorisation           OAuth / user accounts
```

Redis is out because nothing in V1 needs it and `shortn` already covers Redis — the rate
limiter uses Postgres instead, and the cost of that is recorded in phase 2 rather than hidden.
Tracing is out until there is a latency question worth answering.

**Multi-tenancy is out, and this is the one to say out loud before being asked.** V1 has
authentication (who is calling) but not authorisation (what they may touch): a valid API key
can act on any account. Adding it means `tenant_id` on every table and a scoping check on
every read path. Deliberate trade — that weekend went to correctness under failure.

⚠️ **Scope was already expanded once, on day zero** (auth, rate limiting and the evidence rule
were added on 2026-09-19 before any code existed). That is the exact behaviour that killed
`card-engine`. Phase count stayed at seven on purpose. **Nothing else gets added to V1 until
phase 4 is running.**

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
- **No claim without evidence (section 7).** A phase that has not produced the number or the
  failed test behind its architectural claims is not finished, and the next phase does not
  start.
- **The scope fence is closed until phase 4 runs.** It was opened once on day zero; that was
  the allowance, not a precedent.
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

**2026-09-19 (same day, second session) — plan amended. Still nothing built.**
Reviewed the plan against "is this good enough for a senior/staff portfolio?" Verdict: the
idea is good (7.5/10 as designed, 9/10 if Chaos Mode ships) and further design work has near-
zero return. Four gaps were found that the plan did not address; three were closed, one was
recorded as a known limitation:
- **Added section 7, the evidence rule** — his own framing, and the strongest change made:
  every architectural word in the README must be earned by a measurement or a failure case.
  Chosen over a separate Phase 8 benchmarking phase, so the phase count stays at seven and the
  numbers get produced while the context is fresh.
- **Added API-key auth to phase 2** — authn only, hashed keys with a visible prefix.
  Multi-tenancy explicitly rejected for V1 because `tenant_id` would touch phase 1's schema and
  every read path; the limitation is now stated in the scope fence rather than left to be
  discovered in an interview.
- **Added Postgres-backed rate limiting to phase 2** — chosen over in-memory (wrong with two
  instances) and over Redis (contradicts the stated reason Redis is out). It is the phase 3
  concurrency race in a different costume, deliberately.
- **Recorded the MockBank credibility gap in phase 4** — we write both sides, so no real
  integration was ever fought. Mitigation: build MockBank to a written API contract and code
  the client against that document, not against the implementation.
⚠️ Scope was expanded on day zero with no code written — named in the scope fence as the
`card-engine` failure mode. Phase count held at seven; fence closed until phase 4 runs.

**Next session — Phase 1:** the three tables, the balanced-transaction writer, the derived
balance, and the two tests (books balance; float loses money). Auth and rate limiting are
phase 2 — do not start them early.
