# Mock card network — v1

`mock-network` simulates the card network (Visa-like) that sits between merchants and us,
the card **issuer**. The network decides our inputs: it asks us to approve authorizations
under a deadline, retries them, duplicates them, approves on our behalf when we are too
slow, cancels them with reversals (sometimes before the authorization arrives), and days
later sends a clearing file whose amounts differ from what we approved.

**This document is the contract.** Issuer code (phase 11 onwards) is written against it, not
against the simulator's source, for the same reason as [MockBank](mockbank-api.md): both
ends were written by the same person, so no real network ever surprised us. **We test only
the failure modes we thought of.** Coding against a document at least forces the issuer to
handle everything the document permits, including behaviour the simulator does not produce
today.

Consequences:

- Every message may arrive more than once, and in any order. Handle both.
- A deadline is a deadline: an answer after it does not count, and the network will act
  without it.
- An advice is not a question. The issuer must honour it.
- No card numbers, ever. A card is our `card_id`.

---

## Conventions

| | |
|---|---|
| Direction | the network calls the issuer (`ISSUER_URL`); the issuer never calls the network |
| Content type | `application/json` on every request and response |
| Amounts | integer **minor units**. `5000` is $50.00 |
| Currency | `USD` only |
| Timestamps | RFC 3339 UTC, e.g. `2026-10-17T10:00:00Z` |
| Identifiers | opaque strings (`auth_…`, `rev_…`, `adv_…`, `clr_…`). Do not parse them |
| Card | the issuer's `card_id`. No PAN, CVV or PIN appears anywhere |

### Signatures

Every request carries:

- `X-Network-Timestamp`: send time in decimal Unix seconds;
- `X-Network-Signature`: `v1=` followed by the hex HMAC-SHA256 of the timestamp, one ASCII
  dot, and the **exact raw body**, keyed with `NETWORK_SECRET` (at least 32 bytes).

The scheme is the MockBank webhook scheme (`internal/webhooks`) with different header names
and a separate secret. Verify before decoding; accept timestamps within five minutes of your
clock; answer `401` to anything that fails. Each attempt is signed afresh; retries keep the
same body.

---

## POST {ISSUER_URL}/network/authorizations

"May this card spend this much at this merchant?"

```json
{
  "auth_id": "auth_3f81aa02c9d4",
  "card_id": "card_9f2c4a71e0b8",
  "amount": 5000,
  "currency": "USD",
  "merchant": {"id": "mer_coffee", "name": "Corner Coffee", "mcc": "5814"},
  "kind": "initial",
  "sent_at": "2026-10-17T10:00:00Z"
}
```

| field | notes |
|---|---|
| `auth_id` | the network's id for this authorization. **The deduplication key.** A retry or duplicate has the same `auth_id` and the same body |
| `kind` | `initial`, or `incremental` (raise an earlier authorization, e.g. a hotel adding nights) |
| `original_auth_id` | present only for `incremental`: the `auth_id` of the `initial` authorization being raised. The incremental `amount` is the **increase**, not the new total |

### Answer

```json
{"auth_id": "auth_3f81aa02c9d4", "decision": "approved", "approved_amount": 5000}
{"auth_id": "auth_3f81aa02c9d4", "decision": "declined", "approved_amount": 0, "decline_reason": "insufficient_funds"}
```

- `200` only. `approved_amount` may be lower than `amount` (partial approval) but not higher
  and not zero when approved. `decline_reason` is free text for the issuer's records.
- The same `auth_id` must get the same answer every time it is asked.
- Anything else counts as **no answer**: a non-`200` status, a body that does not parse, a
  wrong `auth_id`, an impossible amount, a transport error, or no response within the
  deadline.

### Deadline, retry and stand-in

1. The network waits `NETWORK_AUTH_DEADLINE` (e.g. `2s`) per attempt.
2. No answer → **one retry with the same `auth_id` and body**.
3. Still no answer → **stand-in**: the network decides alone. It approves amounts up to
   `NETWORK_STANDIN_LIMIT` in full and declines anything above it.
4. A stand-in approval is reported to the issuer afterwards with an **advice**. A stand-in
   decline is not reported (nothing moved).

An answer that arrives after the deadline is discarded, even if it was a decline. The
merchant has already been told the network's decision, and the issuer must reconcile to it:
the stand-in approval will appear in clearing whatever the issuer thinks.

---

## POST {ISSUER_URL}/network/reversals

"Cancel (part of) an earlier authorization." The merchant voided the sale or the terminal
gave up.

```json
{"reversal_id": "rev_0b1c2d3e4f50", "auth_id": "auth_3f81aa02c9d4", "amount": 5000, "sent_at": "2026-10-17T10:00:00Z"}
```

- `amount` is the amount being released. A reversal for the full authorized amount means the
  authorization will not clear.
- **A reversal may arrive before the authorization it cancels.** The issuer must remember it
  and apply it when (if) the authorization arrives.
- Answer `2xx` to acknowledge, including for a duplicate `reversal_id`.

## POST {ISSUER_URL}/network/advices

"I approved this on your behalf while you were not answering."

```json
{
  "advice_id": "adv_3f81aa02c9d4",
  "auth_id": "auth_3f81aa02c9d4",
  "card_id": "card_9f2c4a71e0b8",
  "amount": 5000,
  "merchant": {"id": "mer_coffee", "name": "Corner Coffee", "mcc": "5814"},
  "decision": "approved",
  "reason": "stand_in",
  "sent_at": "2026-10-17T10:00:04Z"
}
```

- Sent once per stand-in approval. The issuer must **honour** it (place the hold even if it
  would have declined, even if the card is now frozen) and answer `2xx`, including for a
  duplicate `advice_id`.
- An advice may arrive after the issuer has already answered the same `auth_id` late. The
  network's decision is the advice, not the late answer.

### Delivery of reversals and advices

- `2xx` acknowledges. **Any other status**, a transport error or no response within the
  deadline is retried after 1s, 2s, 4s, 8s and 16s (six attempts), then given up and
  recorded in the network's journal. Answering `503` to a reversal for an authorization the
  issuer has not seen yet is legitimate: it asks for redelivery.
- A given-up message is still part of the network's record: a given-up advice still clears.

---

## Clearing file

The network's final record of what was spent, sent days after the authorizations. The
issuer turns holds into ledger entries from it (phase 13).

```http
GET /clearing?date=2026-10-18
```

```csv
clearing_id,auth_id,card_id,kind,amount,currency,merchant_id,cleared_at
clr_1a2b3c4d5e6f,auth_3f81aa02c9d4,card_9f2c4a71e0b8,purchase,6000,USD,mer_coffee,2026-10-18T10:00:00Z
clr_7a8b9c0d1e2f,,card_9f2c4a71e0b8,purchase,1200,USD,mer_transit,2026-10-18T10:00:00Z
```

| column | notes |
|---|---|
| `clearing_id` | unique per line. The deduplication key for clearing |
| `auth_id` | the authorization being cleared (for an incremental chain, the `initial` one). **May be empty**: an offline purchase that was never authorized |
| `kind` | `purchase` or `refund` |
| `amount` | positive. **May differ from the authorized amount** (tip, fuel, partial shipment). For a chain it is the final total |
| `cleared_at` | the line belongs to the file of this UTC date |

Rules:

- Built from the network's own record: approved authorizations (including stand-in) clear;
  declined ones do not; an authorization reversed in full does not clear, a partly reversed
  one clears the remainder before any tip or partial adjustment.
- A refund references the original purchase's `auth_id` when known and may arrive weeks
  after the purchase. Refunds are never authorized first.
- Lines are ordered by `cleared_at`, then `clearing_id`. The same date asked twice returns
  the same bytes, for as long as no new activity clears on that date.
- `400` if `date` is missing or not `YYYY-MM-DD`. A date with no activity returns only the
  header line.
- The simulator also writes the file to disk: `mock-network scenario … -clearing-dir DIR`
  writes `clearing-YYYY-MM-DD.csv` for each date the scenario touched.

---

## Settlement statement

What the network says the issuer owes it for one date: the net of that date's clearing
file. The issuer pays it once a day and reconciles against it (phase 15).

```http
GET /settlement?date=2026-10-18
```

```csv
settlement_date,purchases_total,refunds_total,net_amount,line_count,currency
2026-10-18,7200,0,7200,2,USD
```

| column | notes |
|---|---|
| `purchases_total` / `refunds_total` | sums of the `purchase` and `refund` lines of the clearing file for `settlement_date` |
| `net_amount` | `purchases_total − refunds_total`. **Negative means the network owes the issuer** |
| `line_count` | every line of that clearing file, purchases and refunds |

Rules:

- One row. Built from exactly the lines `GET /clearing?date=` returns for the same date, so
  the two agree; the same date asked twice returns the same bytes while nothing new clears.
- It counts every line the network cleared, including lines for cards the issuer does not
  know: the network charges them whatever the issuer posted.
- `400` if `date` is missing or not `YYYY-MM-DD`. A date with no activity returns zeros.

---

## Sandbox (operator endpoints)

Not part of the contract the issuer codes against.

```http
POST /sandbox/scenarios   {"scenario": "stand_in", "card_id": "card_…", "amount": 5000,
                           "merchant": {"id": "…", "name": "…", "mcc": "…"}, "seed": 42}
GET  /sandbox/journal     every message sent, every attempt, every answer
POST /sandbox/reset       forget everything
GET  /healthz
```

`merchant` and `seed` are optional. A scenario runs synchronously against `ISSUER_URL` and
returns its seed, authorization outcomes, the dates its clearing lines land on, and its
journal entries. **The same seed produces the same identifiers, messages and clearing lines**
(timestamps come from the network clock). Re-running a seed that would reuse an identifier
already in the journal is refused with `409 id_reused`; reset first.

| scenario | what the issuer receives | what clears |
|---|---|---|
| `approve_and_clear` | authorization | the approved amount |
| `clear_with_tip` | authorization of `amount` | approved + 20 % (auth $50 → clear $60) |
| `partial_clear` | authorization of `amount` | 70 % of approved (auth $100 → clear $70) |
| `duplicate_auth` | the same authorization twice | once |
| `timeout_then_retry` | authorization, then the same `auth_id` again (the first answer is lost on the way back) | once |
| `stand_in` | authorization twice (both answers lost), then an advice if `amount ≤ NETWORK_STANDIN_LIMIT` | the stand-in approval; nothing if declined |
| `reversal_before_auth` | a full reversal, then the authorization it cancels | nothing |
| `incremental` | initial `amount`, then incremental `amount/2` on the same `original_auth_id` | one line on the initial `auth_id` for the approved total |
| `offline_purchase` | nothing | a purchase with empty `auth_id` |
| `late_refund` | authorization | the purchase next day, a refund of it 7–30 days later (seeded) |

Purchases clear 24 hours after their authorization was sent.

---

## Configuration

| variable | meaning |
|---|---|
| `NETWORK_ADDR` | sandbox and clearing listener, e.g. `:8082` |
| `ISSUER_URL` | base URL of the issuer, e.g. `http://localhost:8080` |
| `NETWORK_SECRET` | signing secret, at least 32 bytes |
| `NETWORK_AUTH_DEADLINE` | per-attempt deadline, e.g. `2s` |
| `NETWORK_STANDIN_LIMIT` | largest stand-in approval in minor units, e.g. `10000` |
| `NETWORK_SEED` | seed for scenarios run without one, so a whole session replays |

All are required; there are no defaults.

## Limits, stated plainly

- In-memory: a restart loses the journal and every clearing line, like MockBank.
- One network, one issuer, USD only, no interchange fees: the statement net is exactly the
  cleared amounts.
- The failure modes are the ones listed here. A real network has more.
