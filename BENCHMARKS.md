# Benchmarks

Every phase re-runs the same sweep and adds a row here. A number without the command that
produced it does not go in this file.

## How to reproduce

```sh
make bench-http        # HTTP sweep, spread + hot account, disposable payments_bench DB
make bench-balance     # ledger.Balance cost against account history size
```

`scripts/bench-http.sh` drops and recreates **only** a database named `payments_bench*`,
applies all migrations, seeds 1,000 funded source accounts, 1,000 destinations, one hot
account and 3,000 API keys (the per-key limit is 100/min, so keys are rotated to keep the
limiter from capping the load), starts the real `cmd/api` binary, and drives
`POST /v1/transfers` with k6 at fixed arrival rates for 30 s per step.

- **Open model** (`constant-arrival-rate`): arrivals do not wait for earlier responses, so
  a slow server shows up as latency and `dropped` iterations instead of being hidden
  (coordinated omission).
- `spread`: each request picks a random source among 1,000 accounts.
- `hot`: every request spends from the same source account.
- Only the admission path runs: relay, sender and MockBank are not started, so the outbox
  backlog grows. This measures the synchronous API, not end-to-end settlement.
- Raw per-step JSON lands in `bench/out/<label>/results.jsonl` (ignored; holds API keys).

## Environment

Apple M4 (10 cores), 16 GB, macOS. Postgres 16.12 in Docker Desktop (10 vCPU / 8 GB VM),
default settings (`shared_buffers` 128 MB, `synchronous_commit` on). API is one process on
the host with the default pgxpool (10 connections). k6 2.0 runs on the same machine, so
load generator and server compete for CPU. Laptop numbers: use them for **relative**
comparisons between commits, not as capacity claims.

## Baseline: V1 (`d128529`, 2026-10-14)

### Spread across 1,000 accounts

| target rps | achieved | p50 ms | p95 ms | p99 ms | dropped | non-202 |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 100 | 3.88 | 4.77 | 8.33 | 0 | 0 |
| 250 | 250 | 2.27 | 3.65 | 8.21 | 0 | 0 |
| 500 | 500 | 1.64 | 2.92 | 9.08 | 0 | 0 |
| 1,000 | 1,000 | 1.88 | 14.46 | 44.75 | 0 | 0 |
| 1,500 | 1,148 | 1,597 | 3,513 | 3,663 | 7,820 | 0 |
| 2,000 | 882 | 4,491 | 7,714 | 7,831 | 28,325 | 0 |

**Knee ≈ 1,100 req/s.** Past it the system does not degrade, it falls over: p50 goes from
2 ms to 1.6 s, because nothing bounds the queue in front of the 10 pooled connections.

### One hot account

| target rps | achieved | p50 ms | p99 ms | dropped | non-202 | hot account entries after |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 100 | 3.79 | 31.15 | 0 | 0 | ~3k |
| 250 | 250 | 2.68 | 15.93 | 0 | 0 | ~10k |
| 500 | 393 | 1,401 | 4,893 | 1,261 | 0 | ~24k |
| 1,000 | 259 | 12,637 | 16,061 | 18,075 | 0 | ~36k |
| 1,500 | 152 | 16,301 | 29,917 | 32,874 | 73 | ~47k |
| 2,000 | 116 | 26,532 | 43,373 | 48,766 | 2,480 | ~55k |

**Ceiling ≈ 390 req/s and falling.** Throughput is retrograde: the more the account has
been used, the slower it gets. The non-202s are requests the client abandoned after tens
of seconds in the queue (`context canceled` while waiting for a connection or the lock).

### Balance derivation cost (`make bench-balance`)

| entries on the account | `ledger.Balance` per call |
|---:|---:|
| 1,000 | 0.17 ms |
| 10,000 | 0.73 ms |
| 100,000 | 5.98 ms |
| 1,000,000 | 27.1 ms |

### Correctness after the overload

0 unbalanced ledger transactions, 0 transfers without their ledger transaction,
55,287 transfers ↔ 111,576 entries (2 per transfer + 1,002 funding). Performance broke,
money did not.

## What the numbers say

1. **The hot account is a lock held across ~8 round trips plus an O(history) scan.**
   `checkBalance` takes `FOR UPDATE` on the account row, then `SUM`s every entry the
   account has ever had, then inserts transfer, ledger transaction, entries, the
   `ledger_txn_id` update, the outbox row, and commits (WAL fsync) — all while holding the
   lock. Throughput per account ≈ 1 / lock-hold time, and the `SUM` part grows forever.
   At 1M entries the scan alone is 27 ms, so one account tops out near ~35 tps.
2. **One hot account takes down every account.** During the hot run all 10 pool
   connections sat in `Lock:tuple` on the same row. Any other request — a different
   account, a `GET`, an auth lookup — waits behind them. This is a noisy-neighbour outage,
   not a slow account.
3. **No admission control.** No server-side deadline, no bounded queue, no shedding:
   overload turns into 4–40 s latency instead of fast `503`/`429`s.
4. **Every request makes ~11 sequential DB round trips** (auth lookup, rate-limit upsert,
   account validation, BEGIN, lock, sum, 4 inserts/updates, COMMIT). With a 10-connection
   pool that caps spread throughput near 1.1k req/s on this machine.
5. **Latent hot rows not yet exercised:** one API key's rate-limit row (every request from
   that key upserts the same row), and the FK `FOR KEY SHARE` on `acc_settlement_usd`
   taken by every transfer (MultiXact pressure at high concurrency). `FOR UPDATE` on the
   source also conflicts with incoming transfers' FK `KEY SHARE`; `FOR NO KEY UPDATE`
   would not.

## Scaling options, ranked by payoff for this codebase

| # | Option | Fixes | And the cost is |
|---|---|---|---|
| 1 | **Stored balance row, updated in the same transaction** (`UPDATE account_balances SET available = available - $1 WHERE account_id = $2 AND available >= $1 RETURNING …`). Ledger stays the source of truth; a checker proves `balance == SUM(entries)`. | O(history) → O(1); check-and-debit becomes one statement, so the lock is taken by the write itself | a mutable derived number that can drift if any path forgets it → needs a single write path, a DB-level guard, and a continuous invariant check |
| 2 | **Shrink the lock window**: take the lock last (after inserting transfer/entries/outbox), pipeline the statements in one batch, drop the extra `UPDATE transfers` by generating the ledger txn id up front, use `FOR NO KEY UPDATE` | per-account ceiling up several x | statement order becomes load-bearing and must be pinned by a test |
| 3 | **Admission control**: per-request deadline, bounded wait for a pool connection, `lock_timeout` on the account lock, per-account in-flight cap → fast `503`/`429` with `Retry-After` | hot account cannot starve the pool; overload is fast failure, not 40 s hangs | some valid payments are refused under burst; clients must retry with the same idempotency key (already safe) |
| 4 | **Cut per-request round trips outside the transaction**: short-TTL in-process API-key cache, rate limit off the money database (Redis token bucket, or in-memory + periodic sync), fold account validation into the transaction | spread knee up; fewer connections per request | revocation lag = cache TTL; Redis is a new moving part (but has TTL and keeps hot counters off the ledger DB) |
| 5 | **Pool and Postgres tuning**: size pool to DB cores, `commit_delay` for group commit, `shared_buffers`; never `synchronous_commit=off` for money | cheap constant-factor gain | must be re-measured per machine; easy to over-tune for the laptop |
| 6 | **Hot-account sharding** (N sub-balances per account, spend from any bucket with funds) or **per-account single-writer batching** (one goroutine/partition per account commits a micro-batch of spends in one transaction, LMAX/TigerBeetle style) | lifts the single-row ceiling itself | multi-bucket overspend logic and rebalancing; or a batching window that adds latency and needs partitioned routing across replicas |
| 7 | **Data growth**: partition `ledger_entries` by time, archive published outbox rows, retention for `rate_limits`/webhook events | stable index/cache behaviour as history grows | partition maintenance; cross-partition audit queries |
| 8 | **Horizontal**: stateless API replicas, then read replicas for GETs/reconciliation, then shard accounts by id | scale past one box | only after 1–4: today the database, not the API, is the bottleneck, so more API replicas just add more waiters |

Order matters: 1–3 are one phase (the hot path), 4–5 the next, 6–8 only when a measured
number demands them. Card authorization (ROADMAP phase 10, p99 < 100 ms target) is the
same hot path with `available → held`, so it should be built on 1–3, not on V1's `SUM`.

## Regression rule

Each phase's PR must include a re-run of `make bench-http` (same rates) and paste the two
tables. A p99 regression of more than 20 % at the 500 req/s spread step, or a lower hot
ceiling, blocks merge unless the PR states why the trade is worth it.
