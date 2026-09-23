# MockBank API — v1

MockBank is a simulated bank rail. It accepts payment instructions, returns
immediately, and confirms the outcome later by webhook.

**This document is the contract.** The client in this repository is written
against it, not against MockBank's source. That ordering is deliberate and it is
the one mitigation available for a credibility gap we cannot otherwise close:
both ends of this integration were written by the same person, so no real vendor
ever surprised us. Coding against a document forces the client to handle every
response the specification permits, rather than only the ones the current
implementation happens to produce.

Consequences that are easy to skip and should not be:

- Every error listed here must be handled, including ones MockBank may not emit
  today.
- Timing is a range, not a value. Do not write a client that assumes the
  settlement webhook is fast.
- Fields marked *may be absent* must be treated as absent.

---

## Conventions

| | |
|---|---|
| Base URL | `http://localhost:8081` (`MOCKBANK_URL`) |
| Content type | `application/json` on every request and response |
| Amounts | integer **minor units**. `50000` is $500.00. Never a decimal |
| Currency | `USD` only. Any other value is rejected |
| Timestamps | RFC 3339 with timezone, e.g. `2026-09-22T10:04:11.219Z` |
| Identifiers | opaque strings. Do not parse them or assume a length |

**Authentication is out of scope for v1.** A real rail would require mutual TLS
or a signed request. MockBank accepts unauthenticated calls, and the client must
not be written in a way that makes adding credentials a rewrite.

---

## POST /transfers

Submit a payment instruction.

```http
POST /transfers
Content-Type: application/json

{
  "client_reference": "tr_8beb827158d7796676509c2456cb6636",
  "amount": 50000,
  "currency": "USD",
  "source": "acc_company",
  "destination": "acc_vendor"
}
```

| field | required | notes |
|---|---|---|
| `client_reference` | yes | the caller's own id for this payment. Max 255 characters. **This is the deduplication key** |
| `amount` | yes | positive integer, minor units |
| `currency` | yes | `USD` |
| `source` | yes | free text, max 255. MockBank does not validate it against anything |
| `destination` | yes | free text, max 255 |

### 202 Accepted

```json
{
  "provider_ref": "mb_9f2c4a71e0b8",
  "client_reference": "tr_8beb827158d7796676509c2456cb6636",
  "status": "processing",
  "amount": 50000,
  "currency": "USD",
  "received_at": "2026-09-22T10:04:11.219Z"
}
```

**`202`, never `200`.** The instruction is accepted. The money has not moved.
Nothing in this response says the payment will succeed.

### Deduplication — the property the whole integration rests on

`client_reference` is idempotent. **Submitting the same `client_reference` twice
returns `202` with the same `provider_ref`, and moves money once.**

This is what makes a retry after a timeout safe. It is also the reason a client
must send a stable reference per payment rather than a fresh one per attempt.

A caller must not depend on this behaviour as its only protection. A rail that
loses its deduplication table, or dedupes only within a window, is a rail that
pays twice — so the correct response to a timeout is still "I do not know",
not "retry, the provider will sort it out".

### Errors

| status | `code` | meaning | retry? |
|---|---|---|---|
| 400 | `invalid_request` | malformed body, missing field, bad type | **no** — fix the request |
| 422 | `rejected` | well-formed but refused: insufficient funds at the rail, blocked account, limit exceeded | **no** — terminal |
| 409 | `reference_conflict` | `client_reference` reused with *different* amount, currency, source or destination | **no** — caller bug |
| 429 | `rate_limited` | too many requests. `Retry-After` header in seconds | **yes**, after the delay |
| 503 | `unavailable` | MockBank is degraded or a downstream is down | **yes**, with backoff |
| 500 | `internal_error` | unexpected | **yes**, with backoff |

Error body:

```json
{ "error": { "code": "rejected", "message": "insufficient funds at the rail" } }
```

**`422` is terminal and is not a failure of the request.** It means the rail
considered the payment and declined it. The client must record it as a decision,
not retry it.

### Timeouts — read this twice

MockBank normally responds within 50ms, but **a response is not guaranteed**. A
request may:

- be received, processed, and have its response lost;
- be received and processed **slowly**, arriving after the client gave up;
- never arrive at all.

**A client that times out has learned nothing.** All three cases look identical.
In particular, a timed-out `POST /transfers` **may still have moved money**.

The specified behaviour for a client:

1. Do not treat a timeout as a failure.
2. Do not retry blindly on the assumption that deduplication will save you.
3. Resolve it by asking: `GET /transfers/{provider_ref}` if a reference is
   known, or wait for the webhook.

Recommended client deadline: **5 seconds**. MockBank does not cancel work when a
client disconnects — an instruction accepted before the disconnect is still
processed.

---

## GET /transfers/{provider_ref}

Current state of a payment. **This is the endpoint that rescues an unknown
outcome**, so it is specified to be safe to call repeatedly.

```json
{
  "provider_ref": "mb_9f2c4a71e0b8",
  "client_reference": "tr_8beb827158d7796676509c2456cb6636",
  "status": "settled",
  "amount": 50000,
  "currency": "USD",
  "received_at": "2026-09-22T10:04:11.219Z",
  "settled_at": "2026-09-22T10:04:13.884Z",
  "failure_reason": null
}
```

| status | meaning |
|---|---|
| `processing` | accepted, outcome not yet decided |
| `settled` | the money moved. Terminal |
| `failed` | the money did not move and will not. Terminal. `failure_reason` is set |

`settled_at` is **absent or null** unless the status is `settled`.
`failure_reason` is absent or null unless the status is `failed`.

`404` with `code: "not_found"` if the reference is unknown. A `404` does **not**
prove the payment never happened — a reference the client never received cannot
be looked up. Only a `client_reference` lookup or the webhook can close that gap.

### GET /transfers?client_reference=...

Look up by the caller's own reference, for the case where the `provider_ref` was
never received because the submit call timed out.

Returns the same body, or `404`. **This is how a client resolves a timeout it has
no reference for**, and any client that does not implement it has an
unrecoverable state.

---

## Webhooks

MockBank sends the outcome to a URL the operator configures (`WEBHOOK_URL`).

```http
POST /v1/webhooks/mockbank
Content-Type: application/json
X-MockBank-Event-Id: evt_3f81aa02c9d4

{
  "event_id": "evt_3f81aa02c9d4",
  "provider_ref": "mb_9f2c4a71e0b8",
  "client_reference": "tr_8beb827158d7796676509c2456cb6636",
  "status": "settled",
  "amount": 50000,
  "currency": "USD",
  "occurred_at": "2026-09-22T10:04:13.884Z",
  "failure_reason": null
}
```

Sent when a payment reaches a terminal state: `settled` or `failed`. Never for
`processing`.

### Delivery guarantees, stated plainly

**At least once.** The same `event_id` may arrive any number of times. A
receiver that is not idempotent will double-count money.

**Unordered.** Events for different payments arrive in no particular order, and
a retried delivery may arrive after a later event.

**A webhook may arrive before the submit response.** MockBank sends the webhook
as soon as the outcome is known, which can be before the client has finished
reading and storing the `provider_ref` from `POST /transfers`. A receiver must
handle an event for a payment it does not yet recognise.

**Timing is not guaranteed.** Normally 1–3 seconds after acceptance, but may be
minutes. A client that treats a missing webhook as failure is wrong.

### What MockBank expects back

| response | meaning |
|---|---|
| `2xx` | accepted. No further delivery |
| `4xx` | permanently rejected. **No retry** |
| `5xx`, timeout, connection refused | retried with backoff: 1s, 2s, 4s, 8s, 16s, then given up |

**Return `2xx` for an event already processed.** A duplicate is not an error, and
answering `4xx` to one tells MockBank to stop retrying a delivery it may still
need to make.

If a receiver does not yet recognise the `provider_ref`, `503` is the correct
answer: it asks for redelivery once the race has resolved.

Signatures are **not** implemented in v1. A receiver must not assume payload
authenticity, and the endpoint should be written so verification can be added
without restructuring it.

---

## Sandbox — operator endpoints

Not part of the payment contract. Used by tests and by the chaos work later.

```http
POST /sandbox/reset           drop all state
GET  /sandbox/transfers       list everything received
```

Scenario injection (forcing timeouts, duplicate webhooks, wrong amounts) is
specified separately and arrives after phase 7. The implementation keeps a
single decision point so scenarios can be added without rewriting the handlers.

---

## Summary for the client author

Handle all of these or the integration is incomplete:

```
202 + provider_ref        the normal path. Store the reference, then wait
422 rejected              terminal. Reverse and mark failed
409 reference_conflict    a bug on our side. Never retry
429 / 503 / 500           retry with backoff
timeout or no response    UNKNOWN. Not a failure. Resolve by GET or webhook
webhook, duplicate        process once, answer 2xx
webhook, unknown ref      answer 503 and let it be redelivered
webhook, never arrives    poll GET. Absence is not failure
```
