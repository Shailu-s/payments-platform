# payments-platform

Payment infrastructure: a backend platform for moving money reliably through external
payment providers. It maintains its own double-entry ledger and stays correct under retries,
concurrency, duplicate events, provider failures, and reconciliation mismatches.

**Status: in design. Phase 1 not started.** See [PLAN.md](PLAN.md).

---

## The problem

External payment providers are slow, unreliable and inconsistent. They time out after
already succeeding. They send the same notification twice. They report a payment settled
before they reported it started. Sometimes they simply disagree with you about what happened.

This platform sits between an application and those providers: one API to move money, and a
permanent, auditable record of every cent that survives everything above.

## The guarantees

Each of these will be backed by a named automated test.

1. The ledger always balances — every transaction's debits equal its credits.
2. The same idempotency key never creates two transfers.
3. Money cannot be overspent by concurrent requests.
4. Duplicate provider events never duplicate financial effects.
5. Every financial state change has an audit trail.
6. Ledger entries are never modified or deleted.
7. Kafka redelivery never causes duplicate processing.
8. Provider discrepancies are always detected by reconciliation.

## Stack

Go, PostgreSQL, Kafka, Docker Compose. Modular monolith plus workers.

## Scope

V1 is one currency (USD) and one simulated bank rail. Deliberately out: cards, crypto, FX,
real bank integration, Kubernetes, fraud, KYC, microservices. The scope fence and the
reasoning are in [PLAN.md](PLAN.md).
