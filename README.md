# payments-platform

Payment infrastructure: a backend platform for moving money reliably through external
payment providers. It maintains its own double-entry ledger and stays correct under retries,
concurrency, duplicate events, provider failures, and reconciliation mismatches.

**Status: phases 1–4 done (ledger, transfer API, idempotency and concurrency, provider
simulator). Phase 5 (Kafka and the transactional outbox) in progress.** See [PLAN.md](PLAN.md).

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

## Running it locally

Needs Docker, Go 1.26, and the `migrate` CLI
(`go install -tags postgres github.com/golang-migrate/migrate/v4/cmd/migrate@latest`).

```sh
cp .env.example .env   # once: every setting lives here; nothing has a fallback
make up            # Postgres :5433, MockBank :8081, Kafka :9092, and the Kafka topics
make migrate-up    # apply db/migrations
make test          # the full suite, against the real Postgres
```

Postgres is on **5433**, not 5432, so it does not collide with a local install.

Every command reads its settings from the environment and **stops at startup if one is
missing** — there are no built-in defaults to silently fall back to. The Makefile loads `.env`
and passes it on, so run commands through `make`. `.env` is git-ignored; `.env.example` is the
committed template.

### Running the system end to end

Five terminals:

```sh
make apikey NAME="local dev"   # once: prints a key, shown only this one time
make run                       # terminal 1 — the API on :8080
make relay                     # terminal 2 — publishes outbox events to Kafka
make sender                    # terminal 3 — sends each transfer to MockBank as its event arrives
make worker                    # terminal 4 — the safety net: sends what the sender missed, after 30s
make demo                      # terminal 5 — walks through the API
```

Two paths can send a transfer, on purpose. The **sender** is the main path and reacts to the
event within a second. The **worker** polls the database and only takes transfers older than
30 seconds, so if Kafka or the relay is down, payments slow down instead of stopping. Both
claim a transfer with the same guarded `UPDATE`, so it is sent once either way.

MockBank already runs in Compose. `make mock-bank` runs it from source instead, on the same
port, so stop the container first (`docker stop payments-mock-bank`).

### Kafka

`make up` starts a single Kafka broker, and a one-shot `kafka-init` container creates the
`transfers` topic with **3 partitions** (it skips the topic if it already exists). Kafka has
no volume, so `make down` deletes every topic and message, and the next `make up` recreates
the topic empty.

Check the topic exists with 3 partitions:

```sh
docker exec payments-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic transfers
```

List every topic:

```sh
docker exec payments-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Read every message on `transfers` from the start, showing key, partition and headers (Ctrl-C to stop):

```sh
docker exec -it payments-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transfers --from-beginning \
  --property print.key=true --property print.partition=true --property print.headers=true
```

List consumer groups, then see how far one group has read on each partition:

```sh
docker exec payments-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list
docker exec payments-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group <group-name>
```

If `kafka-init` failed, its output says why:

```sh
docker logs $(docker ps -aqf name=kafka-init)
```

## Scope

V1 is one currency (USD) and one simulated bank rail. Deliberately out: cards, crypto, FX,
real bank integration, Kubernetes, fraud, KYC, microservices. The scope fence and the
reasoning are in [PLAN.md](PLAN.md).
