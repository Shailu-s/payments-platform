# Payments Platform — what we are building and how

**Started:** 2026-09-19
**Language:** Go. **Database:** PostgreSQL. **Events:** Kafka. **Local:** Docker Compose.
**Repo:** standalone, public, GitHub identity `Shailu-s` (set `user.name`/`user.email`
**locally** in this repo — the global config is the Qiro work identity and must never land
on a portfolio commit).

---

## 🚨 Working method

**Changed 2026-10-14: orchestrator plans and writes a brief, executor builds and opens the
PR, orchestrator reviews and re-runs tests and benchmarks.** Shailendra approves each
phase plan, reads the merge report and replies "merge" before any phase PR merges; the
per-commit explain-back gate below is retired. Every phase re-runs the benchmarks in [BENCHMARKS.md](BENCHMARKS.md).

**Changed 2026-10-05: Claude writes, Shailendra reviews and explains.**

- Claude writes each piece into the repo in small, tested changes before it is shown.
- Before review, explain conversationally with a concrete payment example: the constraint
  and why we need the piece, what changed, actual code/data flow, failure tests and cost.
  Give enough detail to understand, one concept at a time; terse file lists or compressed
  summaries are not a substitute for a human explanation (clarified 2026-10-06).
- He reads every diff and **explains the logic back in his own words before any commit or
  push**. Resolve confusion and obtain explicit approval; understanding only the feature
  summary is not enough. Approval of an earlier piece does not cover later changes.
  **Reaffirmed 2026-10-06: no understanding, no commit or push.**
- He still types **one interview-critical piece per phase** by hand.
- **Why the change:** through phase 5 the old method (Claude instructs, he types everything)
  roughly doubled the time per feature — code was written, verified, pasted, retyped, then
  re-reviewed for typing mistakes — and the learning came from the experiments and the "why"
  questions, not the typing. Interviewers ask about every line, so the explain-back is the
  part that cannot be skipped.

*Original rule, 2026-09-19 to 2026-10-04: Claude instructs, he types every line.*

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
promises with the proof attached. A README that opens with what the system *guarantees*, each
line backed by a named test, says more than any coverage percentage.

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

Implemented **two ways** — row locks (`SELECT ... FOR UPDATE`) and `SERIALIZABLE` with retry
on serialization failure — measured, and one chosen **with a stated reason**. Having actually
measured it is the difference between a senior answer and a staff answer.

### The measurement, and the decision

Both are correct: the overspend test passes under either. So the choice is a number.

200 affordable spends against **one hot account**, so anything not carried is a valid payment
the strategy dropped (`TestLockStrategyComparison`):

```
  strategy       conc    tps     p50       p99      ok    lost
  ──────────────────────────────────────────────────────────────
  for_update        1     346    1.5ms    30.2ms   200       0
  serializable      1     799      1ms     5.7ms   200       0     ← faster with no contention
  for_update       10     610   12.1ms    54.1ms   200       0
  serializable     10     279    4.8ms   103.3ms   157      43
  for_update       50     778     60ms    70.5ms   200       0
  serializable     50     122  241.7ms   621.7ms   128      72     ← 36% of payments lost
```

**Chosen: `SELECT ... FOR UPDATE`.** Not because it is stronger — because on a hot account it
is the only one that does not lose payments. At 50 concurrent writers `SERIALIZABLE` exhausted
its retry budget on 72 of 200 valid payments while the row lock carried all 200, at six times
the throughput and roughly a ninth of the p99.

The shape of the numbers is the explanation:

- **With no contention `SERIALIZABLE` wins** (799 vs 346 tps). There is nothing to lock, so the
  lock is pure overhead. That is the case it is built for.
- **Its p50 stays low while its p99 explodes** (4.8ms / 103ms at 10). Most transactions never
  conflict and are fast; the ones that do get aborted and retried repeatedly, so the average
  hides a bimodal distribution.
- **The row lock degrades smoothly.** p50 rises as requests queue, but nothing fails.

**The trade in one line:** blocking makes contention *slow*, aborting makes it *fail*. For
money, slow is the right failure.

**What would change the answer:** contention spread across many accounts rather than
concentrated on one. `SERIALIZABLE` never blocks in the common case and its abort rate follows
real conflicts rather than lock queues. One account with many writers is its worst case and
the row lock's best — and it is exactly the shape a payments hot path has.

**And the cost is:** `FOR UPDATE` serialises all spending from a single account, so one
high-volume account is a throughput ceiling that no amount of application concurrency gets
past. The fix at that point is not a different isolation level but a different data model —
per-account sharding, or balance snapshots that shorten the locked window.

**Only the row lock ships.** The serializable path was deliberately *not* kept in the handler:
a second production path that never executes is where bugs hide unnoticed. The comparison
survives as a self-contained test, so the numbers above can be re-derived rather than
believed. The production transaction runs at Read Committed, so nothing raises `40001` and no
retry loop exists — raising the isolation level later would mean reintroducing one.

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

**Redis is out of V1, and the honest reasoning matters more than the decision.** Rate limiting
in the primary database is *not* what a production system does: Redis gives you TTL, so expired
counters delete themselves and there is no cleanup component at all, and it keeps a hot-path
write per request off the database that holds the money. The V1 choice is Postgres because the
only alternative available was an in-memory counter, which is silently wrong the moment there
are two instances — so the comparison is Postgres against something provably broken, not
Postgres against Redis. The cost is measured in phase 2 rather than hidden, and the number is
what says when this stops being acceptable.

**Redis is the first thing added in the scalability pass** (see V5), for rate limiting and
anywhere else a hot-path counter or cache belongs outside the primary database.

Tracing is out until there is a latency question worth answering.

**Multi-tenancy is out, and this is the one to say out loud before being asked.** V1 has
authentication (who is calling) but not authorisation (what they may touch): a valid API key
can act on any account. Adding it means `tenant_id` on every table and a scoping check on
every read path. Deliberate trade — that weekend went to correctness under failure.

✂️ **Scope cut on 2026-10-05 (fast-track), to finish V1 in ~4–5 sessions:**

```
Phase 5     finish: demote the poller, the SIGKILL test. Kafka-down = one manual run, noted.
Phase 6     inbound: HMAC signature + replay window only (dedupe, status guard, unknown
            transfer already exist from phase 4). Outbound: one consumer with retries,
            backoff and a DLQ status. CUT: manual replay, delivery history.
Phase 7     kept in full.
Chaos Mode  CUT as an API. duplicate_webhook and timeout_after_success become tests.
Dashboard   CUT.
README      kept: the eight guarantees, each tied to its test, plus the evidence numbers.
```

⚠️ **Scope was already expanded once, on day zero** (auth, rate limiting and the evidence rule
were added on 2026-09-19 before any code existed). That is the exact behaviour that killed
`card-engine`. Phase count stayed at seven on purpose. **Nothing else gets added to V1 until
phase 4 is running.**

**Superseded 2026-10-13: the post-V1 plan now lives in [ROADMAP.md](ROADMAP.md) (debit card backend, then stablecoin top-up, then a credit limit).** Original list kept below for history.

Later versions, strictly after V1 is complete and only if wanted:

```
V2  a real payment provider
V3  FX + multi-currency (expiring quotes, the race when a quote dies mid-conversion)
V4  a stablecoin rail  ← the differentiator: settlement that is slow AND probabilistic
                          (confirmations, reorgs, gas failure). Mentioned in interviews,
                          never the pitch.
V5  scalability pass: Redis for rate limiting and hot-path counters, load testing at scale
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
- **Small, reviewable commits** following the build order. Avoid voluntary AI attribution;
  tool-required metadata is permitted (approved 2026-10-05), with `Shailu-s` /
  `srajawat024@gmail.com` retained as the commit author. After the first commit, stop and let
  him review before continuing unless he explicitly approves the batch.
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
- Chose: the eight guarantees as the README centrepiece — specific promises with tests
  attached, rather than a coverage number.
- Kafka kept, but for the honest reason (a named learning gap + the outbox story), not
  because V1 volume needs it.
- Standalone public repo, identity `Shailu-s` set locally.

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

**2026-09-21 → 2026-10-05 — phases 1–4 done, phase 5 nearly done.** The detailed build log
moved to a local `PROGRESS.md` on 2026-09-21 and this LOG was not kept in step; this entry
closes the gap. Phases 1–4 merged (PRs #1–#4). Phase 5 on `phase-5-kafka-outbox`: outbox,
Kafka in compose, relay (with a crash-republish test), consumer loop, `watch`, `SendByID` with
the claim guard (watched double-paying 29 of 50 transfers in its naive form first),
`transfer-sender`, and every setting moved to required environment variables.
**Decided 2026-10-05:** the working method changes to "Claude writes, he reviews and explains",
and V1 is fast-tracked — see the working method section and the scope cut above.

**2026-10-05 — poller demoted; comment cleanup committed.** `8151e4b` gives the event
consumer a 30s head start. Comment-only cleanup `f914551`: 61 Go files passed comment-free
parser/formatter and token comparisons against the preceding HEAD; 15 non-Go files retained
identical non-comment content. Comment lines 1,673 -> 579. Build and full tests passed
against healthy Compose services. Three pre-existing gofmt issues were left outside scope.
The webhook event record and financial transition commit separately: failed application
can be skipped on redelivery; logged as an unfixed gap in PROGRESS.md.
The user deferred comment explain-back and approved commit/push, permitting tool-required
attribution metadata while retaining his Git author identity. Rules updated accordingly;
this rules/log update is separate from the comment-only cleanup. Next implementation:
scripted sender SIGKILL/redelivery proof (5.5); no new phase-5 feature code written yet.

**2026-10-05 — phase 5 failure proof implemented, tested, approved for commit.**
`make chaos-kill-consumer` SIGKILLs an isolated child running production consumer/sender
logic after provider-reference persistence and before offset commit. Negative control:
6 submissions for 5 transfers without the claim guard. With SendByID: 5 submissions,
same record redelivered with sent=false, unchanged entries on replay, five settlements.
Three repeated runs, full build/tests and race detector passed. Kafka-down experiment:
poller submission after 30.233s, callback settlement verified, unpublished event retained;
Kafka restored with existing topics intact. This is a controlled-provider test, not a
production rebalance or MockBank settlement-latency measurement. The earlier provider
acceptance/reference-persistence crash window and webhook two-commit gap remain documented
limits. The user explained the two failure scenarios and approved the commit. Next is
phase 6, starting with atomic webhook deduplication/application before HMAC verification.

**2026-10-05 — phase 5 committed; first phase 6 fix tested, awaiting review.** Commit
`b722f5a` contains the failure demonstrations (now pushed in PR #5). Reproduced the inbound
webhook split-commit bug: HTTP 500 left the event marked seen, so retries skipped the
financial effect. Event recording, state transition and ledger now share one transaction;
duplicates use ON CONFLICT DO NOTHING without aborting it. Regression tests cover ledger
write and final COMMIT failure for settlement and refund, then retry and duplicate delivery.
Build, full tests, vet and targeted race tests passed. The fix is uncommitted. Next: review,
then inbound HMAC/replay protection; outbound retries/backoff/DLQ remains after that.

**2026-10-05 — phase 5 PR opened; phase 6 paused until merge.** User clarified that each
phase's PR must merge into main before the next phase begins. Pushed through `e74a0aa`,
including phase-5 proofs and a separate formatting-only cleanup, and opened PR #5 with gh:
https://github.com/Shailu-s/payments-platform/pull/5. Verified the committed phase-5 snapshot
in an isolated worktree: build, full tests, vet, SIGKILL race test and formatting checks pass.
PR is open, not merged. Phase-6 source edits and this later log remain uncommitted and are
excluded from the PR. Next: review/merge phase 5, then preserve those edits on a new phase-6
branch before continuing. Local CLAUDE.md now explicitly records the merge gate.

**2026-10-05 — user merged PR #5; stopping until the next session.** Verified GitHub main
at `9d0639545161581d815cbdc9fcbc22ef683ab0ca`. Phase 5 is merged. Phase-6 atomicity edits
remain local and uncommitted; no branch switch or further feature work. Next session:
create a phase-6 branch from updated main without losing those edits, review/commit the
atomicity fix, then inbound HMAC/replay protection and outbound retries/backoff/DLQ.

**2026-10-06 — working method reaffirmed before resuming phase 6.** For each piece, explain
the constraint/why, what changed, actual code/data flow, tests and cost in short steps.
Confirm understanding through explain-back and resolve confusion before explicit approval
for any commit or push. Project docs/memory reread; phase 5 merge and preserved phase-6
edits verified. Only workflow documentation changed; no new application code or Git action.
Next: time budget, safe phase-6 branch setup, then explain the prepared atomicity fix.

**2026-10-06 — phase-6 branch created safely; first fix ready for code review.** With more
than 3 hours available, created phase-6-webhooks from merged origin/main at 9d06395 after
checking tree equality; preserved all existing edits. Re-verified the prepared atomicity
fix with build, full tests, vet and targeted race tests. No new feature code or commit/push.
Next: explain the actual transaction/duplicate/rollback code and confirm understanding
before approval; HMAC/replay work waits until that piece is reviewed.

**2026-10-06 — atomic webhook fix reviewed and approved for commit/push.** Walked through
shared transaction/rollback, INSERT ON CONFLICT and its row-count result, and pgx savepoints.
He explained that a rolled-back attempt leaves the retry as a first attempt, a waiting
second delivery processes after rollback, and rejection of the outer COMMIT undoes both
event and ledger changes. Failure tests/cost explained; explicit commit/push approval given.
Next piece: inbound HMAC signature and replay window, tested then explained before any
further commit/push.

**2026-10-06 — atomicity pushed; inbound HMAC/replay slice tested, awaiting code review.**
Approved atomicity commit `4a19435` is on origin/phase-6-webhooks. Added HMAC-SHA256 over
signed delivery timestamp + dot + raw body, an inclusive five-minute clock window, and
verification before JSON/DB work. MockBank signs each retry afresh. First red test showed
an unsigned request crediting 50000 cents; it now returns 401 without an event or credit.
Matching/mismatched-key real-handler HTTP tests, full tests, vet, formatting and targeted
race tests pass. Expanded race checks also exposed a pre-existing live-pointer response
bug in MockBank Submit; snapshot returns and a failing-first regression fix it.
HMAC/snapshot changes remain uncommitted pending explanation/understanding/approval.
Required local signing key and safe MockBank rollout are pending user action; no secret
values exposed, local .env edited or containers restarted. Outbound delivery remains next.

**2026-10-06 — inbound signing reviewed; commit/push approved.** Explained the shared-key
HMAC, raw input, signed freshness timestamp, duplicate handling and the full function flow.
He correctly explained that changing a captured request's timestamp while retaining its
signature fails verification because the timestamp is signed. Explicit push approval was
given. Snapshot race fix committed separately as `ca783e5`; signing/config/tests follow in
the next commit. Local key configuration and container rollout remain pending, not silently
performed. The next feature is outbound customer delivery with retries/backoff/DLQ status;
manual replay and delivery history remain cut.

**2026-10-06 — outbound slice 1: terminal events ready, awaiting code review.** First
watched the missing-event test fail: settled/failed transfers had no completion outbox row.
Shared Settle/Fail now write transfer.settled/transfer.failed with a six-field public payload
on the same transaction as state/ledger effects. Duplicate/concurrent/conflicting callbacks
emit no additional completion event. Outbox-insert and final-commit failures leave no orphan
notification; retries succeed after recovery. Build, full tests, vet and targeted race
checks pass. No new schema, settings or broker. Changes uncommitted pending explanation
and approval; customer delivery consumer/retries/DLQ are the following slices. Private
signing-key setup and safe runtime rollout remain pending; no service was restarted.

**2026-10-06 — terminal-event explanation reviewed; commit/push approved.** Used the
customer invoice example and walked through shared Settle/Fail plus the event writer.
He explained that an outbox insert failure rolls back our attempted status/ledger changes,
allowing the callback to retry. Clarified that only actual final transitions emit, the
bank's payment is not rolled back, and MockBank's finite in-memory retry is the current
redelivery source (not a durable inbound inbox). He explicitly approved commit/push and
building customer delivery next. Customer HTTP outages must retry notification work only,
never undo or resend the payment; replay/history remain outside scope.

**2026-10-07 — terminal events pushed; customer delivery ready for review, not committed.**
Pushed approved terminal-event commit 3718b8c before writing delivery code. Added migration
000011 and internal/customerwebhooks: consumer durably saves one current delivery per event
before offset acknowledgment; a separate DB loop sends signed notifications to one configured
customer URL. Stable envelope event_id/event_type/data, X-Payments-Event-Id/Timestamp/Signature
headers, separate required customer key. Retry transport/timeout/5xx/408/429 with exponential
backoff; permanent errors/exhaustion record dead status. Persisted leases/attempt fences
survive worker restart and block stale response overwrites. Response loss can replay, so
customers must deduplicate by event ID. No history/replay API or additional broker/DLQ topic.
Red/green tests, actual outbox→Kafka→consumer→customer 503/recovery proof, full build/tests/vet
and relevant full race suites pass without additional ledger effects. New make webhook-sender
command uses independent customer-webhooks group and drains existing jobs even if Kafka
startup ping fails. Local env, primary schema and running services untouched. Outbound code
uncommitted pending teaching/explain-back/handwritten piece and approval. Private config,
safe rollout, phase-6 PR and merge remain before phase 7.

**2026-10-07 — customer delivery flow reviewed; commit/push approved.** Explained the
actual relay→Kafka→customer consumer→delivery table→HTTP worker path and where each function
lives. He initially applied recovery to the unsafe acknowledge-before-save hypothetical;
clarified no job would exist there. He then correctly identified saving the pending delivery
in DB as the step required before Kafka acknowledgment. Explicit commit/push request given.
Delivery engine/schema/proofs committed as 015941b; command/env wiring follows separately.
No local migration, private-config edit, service restart or external endpoint call performed.
The handwritten claim/retry piece and final local rollout/PR review remain pending.

**2026-10-07 — local setup started; migration 11 applied, keys block startup.** Ran the
additive make -s migrate-up without resetting existing application tables. User specifically
approved MockBank recreation (in-memory sandbox state loss disclosed) and selected a local
test receiver. Despite reporting keys set, a non-disclosing length check still finds both
required signing keys insufficient in the repository's loaded .env. No bank rebuild/API
startup performed yet. Port 8082 is occupied by another process; leave it untouched and
use free port 18082 for our controlled receiver. Asking for private config completion;
live verification, handwritten review and PR remain pending. No secrets displayed.

**2026-10-10 — real local phase-6 flow verified; Docker build fixes await commit approval.**
Private keys/URL now validate without disclosure; migration 11 already applied. Added
.dockerignore before any COPY build step to exclude local secrets. MockBank build failed
because its 1.25 builder disables automatic toolchain downloads while go.mod needs 1.26.0.
User approved pinned golang:1.26.7-alpine and the specific container recreation; build and
rebuild succeeded without changing that restriction. Only approved MockBank memory reset;
PG/Kafka and existing application data preserved. API, relay, customer worker and controlled
receiver ran: fresh $5 sandbox transfer settled once; signed customer event returned 503
then 200, same ID, two requests/one effect, source and destination 500 cents each, exactly
three scoped ledger transactions. Unsigned bank callback rejected 401. No real customer
endpoint used. Build exclusions/version change and this LOG uncommitted; handwritten
claim/retry review and phase-6 PR/merge remain before phase 7. Services remain running.

**2026-10-10 — user approved final commit/push/PR and phase-6 merge.** Short on time,
he explicitly requested merging phase 6 and a phase-7 plan. Handwritten claim/retry exercise
is deferred learning, not marked done and not a merge blocker under this latest request.
Re-reviewed all phase-6 commits, identity and live proof; build, full tests, vet and broad
race suites passed. Strengthened the durable retry test to exactly five failures then
sixth-attempt success, with unchanged ID/body and 1/2/4/8/16-second persisted dates; targeted
race test and permanent-failure dead status both pass. Docker exclusions/version alignment
and final evidence follow as a small commit; create/merge PR through gh without bypassing
checks. Phase 7 is planning only: provider file contract, six classifications, persisted
exceptions and deliberately corrupted fixture; detect differences, never auto-fix money.

**2026-10-10 — phase 6 merged; phase 7 plan only.** Final commit 7c88388 contains safe
Docker build/version alignment and exact five-failure/sixth-success retry proof. Created
PR #6 with gh, reviewed all seven commits and verified head/mergeability. GitHub had no
configured checks; local build/full tests/vet/broad races and live smoke provided evidence.
Merged only the reviewed head, without admin/bypass flags or branch deletion. Verified
PR MERGED and main at e75002e086e93f091a576c0308c56d019c1fa6ed. Local main fast-forwarded;
private env/ignored smoke helper preserved. No phase-7 implementation started. Deferred
handwritten claim/retry learning remains a retrieval task, not a claim of completion.

**Next phase-7 build sequence (keep full six-classification scope):**
1. Define provider settlement CSV fields, matching references and date/cutoff scope first;
   extend MockBank's written contract and add a deterministic report export.
2. First failing integration test: a clean report classifies internal/provider agreement
   MATCHED. Then one behavior at a time for AMOUNT_MISMATCH, MISSING_EXTERNAL,
   MISSING_INTERNAL, STATUS_MISMATCH and DUPLICATE_EXTERNAL. Never silently deduplicate
   external rows or hide invalid input; establish precedence for overlapping differences.
3. Compare against a consistent internal snapshot including submitted processing/unresolved
   records as appropriate to the cutoff, so a lost settled callback is detected rather than
   excluded. Keep integer USD amounts and original evidence from both sides.
4. Persist run metadata and searchable exceptions with internal/provider identifiers and
   evidence; make reruns well-defined without duplicating results or overwriting evidence.
5. Add a manual reconciler command first, a read-only summary/detail path and an operator
   schedule after it runs. No automatic settlement/refund, dashboard or extra broker.
6. Finish with an isolated deliberately corrupted report covering all six outcomes, plus
   input-error/rerun tests, unchanged-ledger assertions and measured rows/counts/runtime.
   Explain/review each small tested commit, open/merge phase-7 PR, then the README evidence.

**2026-10-10 — phase 7 implemented and benchmarked; PR review requested.** User requested
finishing phase 7 and opening a PR for his review, with limited time; no phase-6 work was
needed because PR #6 is already merged. Preserved the existing uncommitted comparison,
parser and report-contract work, then completed MockBank's locked CSV snapshot export,
read-only Repeatable Read internal snapshot with provider cutoff, migration 12, atomic
append-only run/finding evidence, exact-report/run-ID replay and conflict detection,
authenticated summary/detail/filter routes, manual CSV/download CLI and cancellable
recurring download mode. The schedule is process-local, not a durable nightly scheduler.

Observed red tests: missing HTTP export returned 404; missing persisted-run schema rejected
clean reconciliation; failed lookup without a persisted provider reference incorrectly
became MISSING_INTERNAL; whitespace identities were accepted; authenticated inspection
returned 404; download and command entry points were absent. Fixed each path and verified
its regression. The corrupted-report integration retains all six classifications, original
amount/status evidence and unchanged entire financial snapshots. Twenty concurrent retries
retain one original run; a deferred final-commit rejection leaves no partial run, and retry
succeeds. Real MockBank HTTP export detects a lost settled callback without crediting the
destination. Invalid/oversized reports cannot create successful runs. No automatic money fix.

Benchmarks, Apple M4/macOS arm64: three samples of 10,000 rows. Parse 1.77–1.86ms,
comparison 4.83–5.03ms, complete database-backed run 164–270ms (median 189ms), about 38.18MB
allocated/run and 5.54MB additional persisted storage/run. Database fixture setup and HTTP
download excluded; each database sample contains three runs, not enough for capacity/p99
claims. Re-ran the existing hot-account comparison three times: at 50 writers FOR UPDATE
carried 200/200 at 700–1340 spends/s, while SERIALIZABLE carried 137–144/200 with 56–63
failed attempts. Rate limiter 0.171–0.253ms vs bare round trip 0.101–0.144ms. README records
scope, reproduction commands, evidence and limits; make bench/bench-reconciliation added.

Verification uses a freshly created disposable database rather than resetting the existing
application/test schemas. Final build, vet, full tests and full race suite all pass, including
the real-bank integration. Go 1.26.0 verified. No primary migration, secret edit, container
restart or merge performed. Disposable verification DB `payments_phase7_verify_a71c4e`
is retained for inspection; application data and services are untouched. User explicitly
selected "Commit, push, open PR", waiving pre-commit explain-back for these three tested
commits only. Publication: `phase-7-reconciliation` against `main`; review and merge remain
with the user, and no merge is authorised. V1's final all-eight-guarantee README audit and
deferred handwritten retrieval work are not claimed done.

**2026-10-11 — phase 7 merge verified; V1 closure implemented and approved for PR publication.**
User reviewed and merged PR #7; verified main merge `aaa436a826bb2908f4f963ffdb941040c78e34f4`.
He approved the proposed closure pass with "do it". Started `v1-closure` from fetched
origin/main with a clean tree. The previous explain-back waiver covered only phase 7;
after final verification he explicitly selected "Commit, push, open PR" for four closure
commits, permitting this batch without pre-commit explain-back but without merge/deployment.
No additional product phase.

Ledger mutation proof failed first: entries and transaction references could be rewritten,
and entry deletion succeeded. Migration 13 rejects ordinary UPDATE/DELETE on both tables;
compensating entries still work. This is not protection against privileged TRUNCATE, DDL or
disabled triggers, and README explicitly states that no restricted production DB role is
provisioned. The guarantee audit also reproduced integer-total overflow: three positive
amounts wrapped to 8, the old writer accepted them against 8 on the other side, and PostgreSQL
proved the transaction unbalanced. Validation now checks before addition, returns a named
overflow error without writes, and still accepts the maximum representable total.

Added the complete normal/timeout-after-success demonstration using real API, outbox relay,
Kafka consumers, sender, MockBank, signed callbacks, durable customer delivery and CSV
reconciliation. Test-only gates withhold the acknowledgement after bank acceptance and hold
the callback until unresolved/no-refund/no-resubmit is verified. A $5 payment has one provider
submission, duplicate callbacks with one settlement effect, 503/200 customer retry with the
same signed event, three balanced ledger transactions/six entries and final MATCHED. Random
schemas, loopback servers and isolated Kafka topics/groups preserve application data and live
services. Topic routing changes only fixture outbox rows; test topics remain for inspection.
Production backoff is independently tested; demo shortens it. Both scenarios passed three
repeated race runs. Added the named creation/settlement/refund audit-evidence proof.

Minimal authenticated /metrics failed first with 404, then passed fixed-cardinality format,
concurrent counts, backlog/latest-run state, payment-quota independence and failed/blocked-DB
checks. HTTP counters are process-local; database states/backlogs/latest-run exceptions are
gauges. Latest-run timestamp distinguishes no run from a zero-exception run. Collection and
authentication share a 2s deadline; failed scrapes do not emit healthy zeroes. No new dependency,
monitoring container, IDs/URLs/keys in labels, dashboard, Chaos API or replay/history feature.
A metrics regression exposed the existing recorder overwriting the first response status with
an ignored later header; fixed after observing incorrect 2xx counts. Informational headers
remain distinct from the final status.

Three Apple M4/Go 1.26.0/local-Postgres samples: scrape with 1,000 rows in each of transfers,
outbox and deliveries took 0.530–1.045ms, ~19KB/172 allocations. Socket/TLS and fixture setup
excluded, production JSON logging sent to a discard sink. Memory-only single-thread observation
benchmark 16.9–23.5ns vs ~1.38ns baseline, 32B/one allocation; not full HTTP or contention
throughput. make bench-metrics and demo-v1 reproduce the new evidence; make bench includes it.
README now links all eight guarantees to named tests, explains startup/customer delivery,
warns about the old destructive demo, retains historical benchmarks and records exact limits.

Build, vet, full tests and full race suite passed in new disposable DB
`payments_v1_verify_b8f2d6`; no primary migration, secret edit or live container restart.
Final build/vet/full tests/full race suite, demo-v1 command, demo-envelope assertions,
formatting and diff checks all pass. Verified README proof names and publication file/secret
patterns. Kafka inspection shows image-created anonymous mounts, not Compose-managed named
broker storage; README states that replacement durability is not proved. Provider contract
now reflects the Chaos API scope cut instead of promising scenario injection after phase 7.
Publication: four dependency-ordered commits on `v1-closure`, targeting `main` for user review;
no merge or deployment is authorised. The source-level V1 closure is complete. Deferred
handwritten retrieval is learning, not represented as completed, and live migration/binary
rollout remains explicit operator work. The verification DB is retained for inspection.

**2026-10-11 — V1 closure PR #8 merged at the user's explicit request.** Verified the
reviewed head `381488c657e876a67f7922c6bd46484dcd7f1ce0`, clean mergeability and no configured
GitHub checks, then merged without admin/bypass flags or branch deletion. GitHub main and
local main now match `439ea09d6ea7dee8277c774d0216a053b98aaa1a`; local update was fast-forward
only. All seven phases and the source-level V1 closure are merged. No new feature work,
primary migration, secret change or live service restart. Migration 13 and updated binary
rollout remain operator actions; no further V1 product feature is required. This merge log
update remains local/uncommitted rather than adding an unapproved fifth publication commit.

**2026-10-12 — local rollout complete; clean parallel sandbox ready for user manual testing.**
User requested finishing rollout/log work, then his manual testing, then benchmarking and an
editable Excalidraw plus code-linked architecture walkthrough. Future stages remain in that
order; no benchmarking or diagram implementation performed during this rollout.

Primary DB was still at migration 11. Non-disclosing checks verified the expected local DB,
valid/distinct signing keys and local service endpoints. Applied additive migrations 12/13;
six transfers and eighteen existing ledger entries were retained. User explicitly approved
snapshotting/recreating only MockBank (one retained settled instruction and provider dedupe
history lost) and resuming five existing unsent sandbox transfers. Saved the provider dump
in ignored `bin/mockbank-pre-v1-rollout-20261011.json`. Postgres/Kafka were not reset.
Started current API/relay/sender/poller/customer worker; all six transfers are now settled,
with fourteen ledger transactions/twenty-eight entries. The five new settlement transactions
all balance. Primary reconciliation records five MATCHED and one MISSING_EXTERNAL, the
previously settled instruction lost from provider memory during the approved replacement.
No automatic money correction or historical row rewrite.

Five resumed customer notifications exhausted six transport attempts and became dead.
A separately approved signed diagnostic POST to the old Python receiver on 18082 reproduced
RemoteDisconnected/no HTTP response while GET still worked; endpoint scheme/port matched.
Its old temporary source/logs were unavailable, so the internal receiver exception was not
claimed identified. User approved replacing only receiver PID 47714; no jobs were reset or
replayed. Ignored `bin/manual-receiver` now verifies HMAC, checks header/body event identity,
deduplicates the signed event ID and exposes read-only receipts on loopback 18082. Its unit
and race checks pass. It is a local testing sink, not new production API scope.

A read-only invariant audit also found a pre-existing October 4 fixture: ltx_demo_funding
credits 1,000,000 cents but debits 50,000, leaving 950,000 imbalance. The current demo script
contains balanced amounts; the historical mutation's origin was not asserted. This defect
was not introduced by the rollout, and the original history was not modified. User selected
a fresh parallel manual sandbox instead of using the contaminated baseline.

Created payments_manual_v1, applied all thirteen migrations, and started a dedicated
payments-manual-kafka (apache/kafka:3.8.0, host 19092, three-partition transfers topic).
Current API/bank/relay/sender/poller/customer worker run separately: API 18080, bank 18081,
Kafka 19092, controlled receiver 18082. The previously rolled-out primary DB, bank and
broker were retained unchanged while creating this parallel sandbox.
Ignored `bin/manual-v1.sh` supplies non-secret sandbox-only overrides without changing .env;
destructive legacy demo/down/redo targets are excluded. Ignored `bin/manual-seed` is restricted
to the dedicated local DB and uses an advisory lock plus the checked ledger writer so seed
retries do not add funding twice. Verified two seed calls leave one transaction/two entries,
zero unbalanced transactions, zero transfers. Source acc_manual_company initially has
100,000 USD cents; destination acc_manual_vendor zero. API/bank readiness and protected
metrics 401 verified; both consumer groups assigned successfully. No payment was created
in this clean sandbox by the agent. User now performs manual tests. Benchmarks and the
actual-flow/function/SQL/failure-path Excalidraw walkthrough follow his results.

Only this LOG changed in tracked source; backups/testing helpers are ignored runtime
artifacts and must not be staged. User explicitly selected "Commit and push log", waiving
explain-back for one PLAN.md-only commit/push to main. Historical dead deliveries remain
intact for inspection. Manual testing is now handed to the user; benchmarking and the
Excalidraw/code walkthrough are still pending his testing results.

**2026-10-14 — V1 benchmarked; workflow changed to orchestrator/executor.** User asked to
benchmark V1, plan for a scalable low-latency system, and switch to orchestrator → brief →
executor → review/merge. Added an open-model k6 harness (`make bench-http`, disposable
`payments_bench` DB only, 1,000 funded accounts, 3,000 keys) and `make bench-balance`.
Baseline in BENCHMARKS.md: spread knee ~1,100 req/s (p99 9 ms at 500, collapse past
1,100); one hot account ceiling ~390 req/s and retrograde as history grows, with all 10
pool connections waiting on that row's lock (noisy-neighbour outage); `ledger.Balance`
0.17 ms at 1k entries -> 27 ms at 1M. No server-side deadline or load shedding. Ledger
stayed balanced after overload (0 unbalanced, 55,287 transfers). User approved: the
hot-path phase before cards (stored balance + one-statement check-and-debit, shorter lock
window, admission control, invariant check, replay-gap fix); executor as a subagent; phase
PRs merge only after his "merge"; the harness PR may merge directly.

**2026-10-14 — Phase 08 (fast, isolated spending path) built on `phase-08-hot-path`; PR
open, not merged.** Executor followed `briefs/phase-08-hot-path.md`. Migration 14 adds
`account_balances` (CHECK balance >= 0, trigger-created rows, backfill that fails on an
already negative account; settlement accounts have no row); `ledger.Record` applies net
deltas last in its single batch, in account-id order, maps the CHECK to
`ErrInsufficientFunds`, and no longer uses a savepoint on a caller's transaction.
`VerifyBalances`/`StoredBalance` added. Migration 15 defers the transfer→ledger FK so
creation inserts the transfer (idempotency key) first and the balance debit last;
`checkBalance`/`SetLedgerTxn` removed; settlement accounts cannot be a source; GET account
serves the stored balance. Admission control: per-process gate (4 in flight per account,
100 ms wait), 250 ms lock timeout set after the key is claimed (at BEGIN it broke
`TestKeyTakenButNotYetCommittedIsSerialised`), 5 s request deadline; `503 account_busy` /
`request_timeout` with Retry-After. Benchmarks (same session as `main`, on battery):
balance read 0.12 ms at 1M entries; hot account 998 accepted/s at 1,000 and 1,212/s at
1,500 with only 503s (main ~330/s); spread knee ~1,400; 0 drift on every step. Missed and
explained in BENCHMARKS.md: strict all-202 hot ceiling (500), mixed spread p99 538 ms at 2x
ceiling, spread p99 at 500 (17–32 ms vs main 13 ms), 500s under spread overload. Root cause
for the misses: refused requests still spend pooled round trips on auth/rate limit before
the gate (next phase). Owner action: migration 14 fails on `payments`
(`acc_demo_funding` -50,000) and `payments_manual_v1` (`acc_manual_funding` -100,000) until
those funding accounts are fixed deliberately.

**2026-10-17 — Phase 09 (cards and card accounts) built on `phase-09-cards`; PR open, not
merged.** Executor followed `briefs/phase-09-cards.md`. Migration 16 adds `card_accounts`
(one held account per funding account, `held_account_id` UNIQUE) and `cards` (status
active/frozen/closed, limits with `daily >= per_transaction`, cosmetic random last4, unique
idempotency key). `internal/cards`: `Issue` inserts the held account in the same statement
that wins `INSERT ... ON CONFLICT DO NOTHING` on `card_accounts` (data-modifying CTE; the FK
is checked at end of statement), so concurrent first cards share one held account and the
loser creates nothing; `Transition` is one guarded `UPDATE ... WHERE status = ANY(allowed)`,
falling back to a read for 404 / 200 no-op / 409 `invalid_card_state`. Issue and every
real transition write `card.*` to the outbox (topic `cards`, added to `kafka-init`) in the
same transaction. Failing first, shown by temporary swaps: a check-then-insert held account
gave each concurrent first card its own held account; a read-then-write freeze reopened a
closed card. Benchmarks (battery, same session as `main`, three alternating runs): spread
p99 at 500 6.2 ms vs main 18.9 ms; hot 1,000 sheds 0–246 on either branch (at the
ceiling, noisy); 0 drift. Open: held accounts are ordinary asset accounts and transfers do
not refuse them yet; limits stored, enforced in phase 11.

**2026-10-17 — Phase 10 (card network simulator) built on `phase-10-mock-network`; PR
open, not merged.** Executor followed `briefs/phase-10-mock-network.md`. Contract first
(`docs/mock-network-api.md`), then `internal/cardnetwork` (messages, `webhooks` HMAC with
`X-Network-*` headers, injectable `Clock`/`FakeClock`, in-memory journal, deadline + one
same-`auth_id` retry + stand-in with advice, reversals/advices retried on any non-2xx at
1/2/4/8/16 s then given up), ten seeded scenarios, and a clearing builder that resolves
scripted lines against the journal (declined and fully reversed authorizations drop out,
incremental chains clear their total on the initial `auth_id`). `cmd/mock-network` serves
`/sandbox/scenarios`, `/sandbox/journal`, `/sandbox/reset`, `/clearing?date=` and has a
`scenario` subcommand that writes clearing files to disk. Decisions within the brief:
timeouts in `timeout_then_retry`/`stand_in` are produced by discarding the issuer's answer
("lost on the way back"), since the network cannot make a real issuer slow; genuine slow
issuers are covered by the real per-attempt deadline. Advice ids derive from the auth id;
re-running a seed that reuses ids is refused (`409 id_reused`). Compose service is behind
profile `network` with `${NETWORK_SECRET:-}`, because `:?` breaks `make up` for everyone
without the secret. No benchmark run (no API path changed). Tests ran against a disposable
`payments_p10_test` database via `TEST_DATABASE_URL`.

**2026-10-17 — Phase 11 (card authorization) built on `phase-11-authorization`; PR open,
not merged.** Executor followed `briefs/phase-11-authorization.md`. Migration 17 adds
`card_authorizations` (`auth_id` PK, decision, `stand_in`, `hold_shortfall`, deferred
`ledger_txn_id`), `card_daily_spend` (card, UTC day), `network_messages` and
`cards.blocked_mccs` (optional on `POST /v1/cards`, sorted/deduplicated; added to the card
fingerprint only when non-empty so pre-phase-11 retries still match). `POST
/network/authorizations` (signature first, `NETWORK_SECRET` now required by `cmd/api`, 1 s
deadline, always the stored row): card read, gate on the funding account, then one
transaction whose first batch is row insert → `SET LOCAL lock_timeout` → outbox event →
conditional daily upsert, then the hold (`available -> held`) last. Duplicates wait on the
PK and replay the stored row; overdraft (CHECK) or daily-limit failure rolls back and one
`INSERT … ON CONFLICT DO NOTHING` CTE (with its outbox event) stores the decline or yields
to a concurrent duplicate's answer. `POST /network/advices`: `network_messages` dedupe,
claim-or-lock the row, daily spend unconditional, both balance rows locked in id order,
hold `min(amount, available)`, rest in `hold_shortfall`. `validateAccounts` refuses held
accounts as either endpoint in the same query. Deviations, all within the decided
behaviour: the card is read before the transaction (an unlocked read inside it is no
stronger) so the gate precedes the connection; daily-limit declines use the
second-statement path like insufficient funds instead of an UPDATE in transaction A, so the
approved event can share the first batch and the counter lock is held one round trip less.
Failing first: held-account transfers (202 → 400), concurrent taps without funds handling
(500s), daily counter without its WHERE (20 of 20 approved past a $100 limit), advices
before the route (404). Benchmarks (battery 28 → 17 %, same session as main): auth-spread
p99 3.2 ms at 500/s, 57/34 ms at 1,000/s, 101 ms at 1,500/s, 0 non-200; auth-hot ~1,500
answered/s with 0.3 % fast 503s; 0 drift. Missed: transfer spread p99 at 500 is +1.0–1.5 ms
over main in three alternating runs (+16–37 %), not isolated. Tests ran against a
disposable `payments_test_p11` database, benchmarks against `payments_bench_p11`; both
dropped.

**2026-10-17 — Phase 12 (reversals, expiry, incremental holds) built on `phase-12-holds`; PR
open, not merged.** Executor followed `briefs/phase-12-reversals-expiry-incremental.md`.
Migration 18: `held_amount` / `released_amount` / `expired_at` (backfilled from
`approved_amount - hold_shortfall`), partial index of open holds, `pending_reversals`. Phase
11 writers set `held_amount` (approval, stand-in). `internal/holds.Release` is the only
release path: row `FOR UPDATE`, `min(amount, held_amount)`, daily counter for the
authorization's UTC day floored at 0, outbox event, ledger `held -> available` last. `POST
/network/reversals`: signature, `network_messages` dedupe, release on the holder (the
initial row for an incremental `auth_id`), else park. Approvals and advices apply parked
reversals after the hold in the same transaction. Incremental: insert-first, lock the
original, decline (`original_not_found` / `original_not_approved` / `original_expired` /
`card_mismatch` / chain over `per_transaction_limit`) by rolling back and storing the
decline in a second statement, else raise the original and hold the increase. Deviations:
(1) a transaction-scoped advisory lock on `auth_id`, taken first by approvals, advices and
reversals, because without it a reversal racing its authorization was lost (1 in 20 pairs,
`TestReversalBeforeAuthorizationIsAppliedOnArrival/racing`); (2) a reversal of a declined
authorization is parked, not dropped, since a later stand-in advice can approve it;
(3) `ExpireBatch` claims and releases one hold per transaction (`LIMIT 1 FOR UPDATE SKIP
LOCKED`) instead of locking the batch in one transaction, which would hold a hot account's
balance lock across the batch and could deadlock with a reversal on another day's counter
row; (4) `incremental_not_supported` removed. Failing first: held_amount 0 before the
writers; release without `FOR UPDATE` hit the CHECK; reversals 404 before the route; no
advisory lock lost a reversal; incrementals declined `incremental_not_supported`; without
the original's row lock 11 increments passed an 8-increment chain limit. Benchmarks (battery
13 → 9 %): auth-spread p99 2.7–15.6 ms at 500, 4.2–10.5 ms at 1,000, 0 non-200; spread p99
at 500 within the rule; expiry 1,400 holds/s; 0 drift. Tests ran in per-package schemas on
the default database; benchmarks against `payments_bench` (dropped).

**2026-10-17 — Phase 13 (clearing) built on `phase-13-clearing`; PR open, not merged.**
Executor followed `briefs/phase-13-clearing.md`. Migration 19: `acc_network_payable_usd`
(settlement, no balance row), `clearing_files` (raw bytes, SHA-256 unique), `clearing_records`
(`clearing_id` primary key, status, charged from held / available, released, shortfall,
comma-separated flag, deferred ledger link), `cleared_amount` / `cleared_at` on
`card_authorizations`; the test harness restores the payable account after truncation.
`clearing.Import`: parse and validate the whole file (header, ids, positive integer
amounts, USD, UTC `cleared_at` on the file's date, no duplicate `clearing_id`), store it
once by hash, then one transaction per line: record inserted first `ON CONFLICT DO
NOTHING`, `auth_id` advisory lock and authorization row `FOR UPDATE` in the same batch
(an incremental `auth_id` resolves to its initial row), balances locked in id order only
when the available account pays, daily counter freed on partial clears, one balanced ledger
transaction last, `card.clearing.posted` event. Lock-timeout/deadlock errors retry the line
up to 5 times. `cmd/clearing`: `-file` / `-download URL` with `-date`, `-list`, `-inspect
[-flag]`. Deviations: (1) a replay of the same bytes still posts lines that have no record
yet (counted by `clearing_id`), so a crash mid-file is finished by rerunning the same file,
as the brief's crash test requires; a complete replay processes nothing; (2) two extra flags,
`nothing_held` (approved, never released or cleared, yet holding 0, e.g. a fully short
stand-in) and `card_mismatch` (line's card differs from the authorization's; the
authorization's accounts are charged); a declined authorization that clears is
`unauthorized_clearing`; (3) `Import` takes a `Config{OverTolerancePct}`; (4) an extra index
`clearing_records (file_id, cleared_at, clearing_id)` for `-inspect`. Failing first
(mutations): the authorization read without `FOR UPDATE` let clearing and a reversal both
take one hold (held_amount CHECK fired in `TestClearingRacingReversalAndExpiry`); `min` removed
from the shortfall overdrew (`account_balances_non_negative` in `TestShortfallNeverOverdraws`);
without the per-line dedupe and the complete-replay check, re-importing posted every line
again (`TestClearingSameFileTwiceChangesNothing`: 2 duplicate_clearing charges) and four
concurrent importers posted 80 of 20 lines. Benchmarks: auth-spread p99 3.7–6.9 ms at
500/1,000, 0 non-200; spread 500 p99 vs main −55 %, +32 %, +157 %, +0.3 % over four pairings
with no transfer code changed (laptop noise, reported in BENCHMARKS.md); import ~1,230
lines/s, replay ~16–25 ms; 0 drift. Tests in per-package schemas on the default database;
benchmarks against `payments_bench` and `payments_bench_clearing` (dropped).

**2026-10-17 — Phase 14 (refunds) built on `phase-14-refunds`; PR open, not merged.**
Executor followed `briefs/phase-14-refunds.md`. Migration 20: `refunded_amount` on
`card_authorizations`, `credited_amount` on `clearing_records`, `clearing_files` unique on
`(file_date, sha256)` instead of `sha256` (PR #15 follow-up), and a partial index on
deferred records. `Import` posts refund lines in the purchase pattern (record claimed,
`auth_id` lock + authorization row `FOR UPDATE`, incremental resolved to its initial row,
card lookup, then one batch for `refunded_amount`, the record and `card.refund.posted`, the
ledger last: debit `acc_network_payable_usd`, credit the line's card's available account).
Matched = auth found with `cleared_amount > 0`; refunds past `cleared_amount` flagged
`refund_exceeds_purchase`; no/unknown/never-cleared auth `unmatched_refund`; unknown card
rejected; card status ignored; holds and daily spend untouched. `clearing.PostDeferred` /
`cmd/clearing -post-deferred` pages deferred records by `clearing_id` and claims each with
`UPDATE … SET status = 'posted' WHERE clearing_id = $1 AND status = 'deferred'` in the
line's transaction (shared `post` with the import's insert claim). Deviations: (1) a
matched refund on a different card than its authorization credits the line's card and is
flagged `card_mismatch`; (2) a refund line with an `auth_id` now takes the auth lock and
row lock (phase 13 skipped them for refunds); (3) `Summary` and `Record` gain `credited`;
(4) the deferred partial index; (5) `PostDeferred` rejects a negative tolerance like
`Import`. Failing first: header-only file for the next date returned the first date's file;
every refund test saw `deferred` records and unchanged balances before the posting code;
`PostDeferred` stub left 22 deferred lines. Benchmarks: auth-spread p99 4.2–10.1 ms at 500,
11.4–57.3 ms at 1,000, 0 non-200, 0 drift, p50 within 0.08 ms of main; refund import
~1,250 lines/s, replay ~16–25 ms. Tests in per-package schemas on the default database
(Kafka started for the Kafka suites, then stopped); benchmarks against `payments_bench` and
`payments_bench_clearing` (dropped).

**2026-10-17 — Phase 15 (network settlement and reconciliation) built on
`phase-15-settlement-recon`; PR open, not merged.** Executor followed
`briefs/phase-15-settlement-recon.md`. Simulator `GET /settlement?date=` and
`cardnetwork.StatementOf` / `SettlementStatement` (contract updated). Migration 21:
`acc_network_cash_usd`, `network_settlements` (PK `settlement_date`, CHECK net = purchases −
refunds, deferrable ledger link), `card_recon_runs` / `card_recon_findings` append-only via
migration 12's `reject_reconciliation_mutation()`, partial indexes on
`card_authorizations (original_auth_id)` and `(expired_at)`. `cardsettlement.Settle`: insert
first (`INSERT … SELECT` totals of posted records whose file has the date, `ON CONFLICT DO
NOTHING`), ledger last; refuses dates with no clearing file or deferred lines.
`cardrecon.Reconcile`: one read-only Repeatable Read snapshot (date's records; authorizations
they name, their incrementals, holds open past `HOLD_EXPIRY_DAYS`, holds expired on the
date; recorded settlement), in-memory classification, run + findings in one transaction with
`COPY`, V1 run-ID replay. Deviations: (1) migration 12's trigger function reused, not
migration 13's (13's raises "ledger history is append-only"); (2) Settle refuses no-file and
deferred-line dates; (3) SETTLEMENT_MISMATCH compares purchases, refunds, net and count, not
only net; (4) two partial indexes added; (5) `card_mismatch` on a purchase is reported as
REFUND_EXCEPTION, as the brief's table says. Failing first: Settle stub failed all four
settlement tests, and without `ON CONFLICT` concurrent callers failed with a primary-key violation;
a stubbed `Classify` failed every reconciliation test; without the triggers UPDATE/DELETE of
findings succeeded. Benchmarks: auth-spread p99 3.0–5.8 ms at 500, 3.7–39.7 ms at 1,000, 0
non-200, 0 drift, p50 within 0.16 ms of main, p99 order-dependent in both directions; Settle
10,000 lines 3.5–5.2 ms; Reconcile 10,000 55–65 ms, 29 MB. Tests in per-package schemas on
the default database; benchmarks against `payments_bench` and `payments_bench_cardrecon`
(dropped).

**2026-10-17 — Phase 16 (benchmark and evidence, V2 closure) built on `phase-16-evidence`;
PR open, not merged.** Executor followed `briefs/phase-16-benchmark-evidence.md`. Commits:
warm-up in `scripts/bench-http.sh` (`WARMUP`, recorded per row); `TestDuplicateAuthorizationsUnderLoad`
and `TestReversalBeforeAuthorizationUnderLoad` (internal/api/chaos_test.go);
`TestAPIKilledMidAuthorizationLeavesNoPartialHold` (cmd/api/kill_test.go: the test binary
re-executes itself as the real `cmd/api` main, SIGKILLed under 16 simulator workers, then
restarted on the same address); `make bench-day` (`scripts/bench-day.sh`, `cmd/benchday`,
`bench/k6/day.js`, `payments_bench_day` only); BENCHMARKS.md phase 16; README rewritten
around the card story; ROADMAP progress (phases 9–15 marked merged) and phase log. Failing
first: primary key dropped → 20 rows/holds/events per auth_id; `ApplyPending` skipped → 0
of 400 reversals released; advisory lock removed → 1 of 400 lost in 1 of 3 `-race` runs
(0 of 10 plain runs); hold in its own transaction before the row → 2 and 7 holds without a
row after the kill. Benchmarks (AC, video call running on the machine, disclosed):
auth-spread p99 9.4 ms at 500, 68.2 ms at 1,000 (median of 3), knee ~1,000/s; hot card
~500 approvals/s clean; card day tap p99 6 → 43 ms during clearing + expiry, books
reconciled, 0 drift. Deviations: transfers run twice single (not 3×); earlier battery runs
recorded separately, not in the medians. Open risks: goodput collapse past the knee (1 s
deadline instead of early shedding); a card-read timeout before the transaction answers
500 instead of 503. Tests in per-package schemas on the default database; benchmarks
against `payments_bench` and `payments_bench_day` (dropped). PR #18.

**2026-10-18 — Phase 17 (shed load early, cache API keys) built on `phase-17-load-shedding`;
PR open, not merged.** Executor followed `briefs/phase-17-load-shedding.md`. Commits: an
admission limit in front of the three network routes and `POST /v1/transfers` (3 × pool
max conns, non-blocking, `503 overloaded` + `Retry-After: 1`, registered before auth so a
refused transfer costs no query); `boundedPool` for those routes (20 ms acquire deadline,
then `ErrOverloaded`; query/transaction on the acquired connection under the request's
deadline; tests keep a plain `DB`); a timed-out read before the transaction mapped to 503
(the transfer account check had flattened its error into a string: 500); `auth.Cache`
(hash-keyed, 30 s TTL, 10,000 entries oldest-first, singleflight per hash, live keys only,
`Revoke` evicts locally and bumps a generation so an in-flight lookup cannot re-cache);
`/metrics` time in `Allow` and in the rate-limited request, k6 p99 of answers and of 503s
apart. Failing first: middleware unwired -> tap/advice/reversal 200 and transfer 202 with
the slots full; unbounded acquire -> 503 after 1.0 s (tap) and 3.0 s client timeout
(transfer); `ErrOverloaded` unmapped -> 500; account-check error flattened -> 500 (the
card-read half already answered 503 on main: context deadlines were mapped); no
singleflight -> 50 queries for 50 misses; no local eviction / no expiry -> revoked key
accepted. Benchmarks (battery, idle machine, same session as main, 2 runs): auth-spread
offered 3,000 -> 1,352/1,903 approved/s vs main 862/60; 2,000 -> 1,926/1,964 vs 2,000/1,814;
p99 at 500 3.7/3.3 ms vs 6.5/3.3; transfer spread 2,000 -> 1,481/1,489 accepted p99 42–44 ms
vs main 1,607/826 p99 4.1/5.0 s; hot transfers 1,351 vs 1,235 accepted/s at 1,500; 0 drift.
Missed: p99 of 503s 277 ms at auth-spread 3,000 in run 1 (32 ms in run 2; CPU saturated
with k6 on the same machine). Costs measured: 0.05–0.4 % refused at 1,000/s (admission
limit during stalls, not the 20 ms bound: 15,977 of 16,131 transfer 503s < 1 ms); the hot
card approves 1,614–1,680/s at 2,000 vs main 1,593–1,946 because gate waiters hold
admission slots. Rate limiter: 20–25 % of a rate-limited transfer's time below the knee
(0.27–0.41 ms per `Allow`), ~30 % above. Recommendation: do step 3 (move the limiter off
Postgres). Deviation: an extra commit for the limiter timing and the k6 fields. Tests in
per-package schemas on the default database; Kafka started for the Kafka packages and
stopped; benchmarks against `payments_bench` (dropped). PR #19.

**2026-10-19 — Phases 16 and 17 merged; flow docs.** PR #18 (phase 16, benchmark and
evidence, V2 complete) merged 2026-10-17; PR #19 (phase 17, load shedding and the API key
cache) merged 2026-10-18. The owner added `SYSTEM_DESIGN.md` (the system on one page, the
scaling steps and their triggers) and `docs/flows/` (request flow diagrams) on `feat/docs`;
they ship with phase 18. ROADMAP progress rows and phase log updated to merged.

**2026-10-22 — Phase 18 (rate limiter in Redis, docs catch-up) built on
`phase-18-redis-limiter`; PR open, not merged (numbers superseded by the next entry).** Executor followed
`briefs/phase-18-redis-limiter.md`. Commits: docs catch-up (phases 16/17 merged, flow docs);
compose service `redis` (redis:7-alpine, no volume, no persistence) and go-redis v9.23.0;
`ratelimit.RedisLimiter` (one Lua script: `INCR`, `PEXPIRE` 2 × window on the first hit,
key `rl:<key id>:<window start unix>`, window computed in Go and shared with the Postgres
limiter, no sweeper); fail open under a 10 ms per-check deadline (`Decision.Degraded`,
`payments_ratelimit_failopen_total`, warnings throttled to one a second, `MaxRetries -1`,
`ContextTimeoutEnabled`); `RATE_LIMIT_BACKEND` (required, redis|postgres) and `REDIS_URL`,
`api.WithRateLimiter`, sweeper only for Postgres, `/readyz` independent of Redis; bench
tooling (`BenchmarkRedisAllow`, bench-http flushes Redis db 15 per mode and records the
backend and fail-open count). Failing first: GET-then-INCR across 4 clients allowed 142 of
200 against 100; `Allow` returning the error -> `TestRedisDownFailsOpen` and
`TestRedisSlowFailsOpenWithinDeadline` fail ("want nil: an error answers 500"); no own
deadline -> 102 ms against a silent Redis (want <= 30 ms); `PEXPIRE` on every hit -> TTL
reset; middleware without the degraded branch -> Remaining/Reset invented, fail-open count
0. Benchmarks (battery, machine NOT idle, load 11–12 on 10 cores, 2 pairs vs main
`dba3f78`): limiter share 26–38 % -> 10–15 % at 500/1,000 (target < 5 % missed); `Allow`
0.53–1.29 ms vs main 0.58–4.45 ms (serial: 0.10 vs 0.17 ms); 2–6 % of checks failed open
below the knee (10 ms read timeouts through Docker Desktop; 0.3–0.6 % even at 50 ms);
spread p99 at 500 43.6/57.3 vs main 50.0/28.8 ms and hot ceiling 871/730 vs 736/851
(noise larger than the effect); postgres backend ≈ main; 0 drift. Open risks: fail-open
rate under load on this machine; the regression rule needs a re-run on an idle machine.
Owner action: add `RATE_LIMIT_BACKEND=redis` and `REDIS_URL=redis://localhost:6379/0` to
`.env`. Tests in per-package schemas and Redis db 14; benchmarks against `payments_bench`
(dropped) and Redis db 15; Kafka started for the race suite and stopped. PR #20.

**2026-10-22 — Phase 18 review fix: warm Redis pool, 50 ms deadline (PR #20).** Orchestrator
review: the 2–6 % fail-open rate was a parameter problem. The pool started empty, go-redis
closes a connection whose call timed out, and a handshake took up to ~27 ms under load, so
one slow moment cascaded (dial, timeout, close, dial). Commit `bd47ea0`: `MinIdleConns` =
admission limit (30 at pool 10, pool size 60), `redisTimeout` 50 ms (detects a broken
Redis, not a busy one; cost: up to 50 ms per rate-limited request while Redis accepts but
does not answer; taps are not rate limited), `DialerRetries = 1` (go-redis redials a refused
connection 5 × 100 ms by default, which held a stopped-Redis check for the whole deadline).
Failing first: no `MinIdleConns` -> `TestRedisPoolStartsWarm` 0 idle; default dial retries
-> `TestRedisDownFailsOpen` 51.7 ms (bound redisTimeout/2); no own deadline -> 102 ms
against a silent Redis (bound 70 ms). Re-run, 2 pairs vs main (battery, `caffeinate -i`):
fail-opens 0–0.13 % of checks at spread 500/1,000 (was 1.8–6.1 %), `Allow` 0.21–0.40 ms vs
main 0.43–2.46, share 12.1–16.8 % vs 24.4–35.4 % (< 5 % not reachable through Docker
Desktop: ~0.1 ms per round trip serially), spread p99 at 500 10.9/21.7 vs 34.8/23.8 ms,
hot ceiling 1,066/888 vs 1,345/950 accepted/s (main alone 804–1,345 at hot 1,500), 0 drift.
