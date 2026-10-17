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
number demands them. Card authorization (ROADMAP phase 11, p99 < 100 ms target) is the
same hot path with `available → held`, so it should be built on 1–3, not on V1's `SUM`.

## Phase 08: stored balance, lock last, fail fast (`71781bb`, 2026-10-14)

Re-run on the same machine and settings as the baseline (pool 10, 30 s steps), but **on
battery, with the editor and browser open (load average ~10 on 10 cores during runs)**: the
`main` binary measured in the same session was slower than the recorded V1 baseline (spread
knee below 1,000 instead of ~1,100, hot ceiling ~330 instead of ~390). So each table puts
this branch next to `main` **measured in the same session** (`28d6988`, with this phase's k6
script for `status_counts`); compare columns, not against the V1 tables above.

```sh
make bench-balance
MODES="spread hot" RATES="100 250 500 1000 1500 2000" make bench-http
MODES=hot RATES="500 1000 1500 2000 3000" HOT_HISTORY=1000000 LABEL=p08-hot-1m make bench-http
MODES=mixed RATES="500 1000 2000" SPREAD_RATE=300 LABEL=p08-mixed make bench-http
```

`balance_drift` (`benchseed -verify` → `ledger.VerifyBalances` after every step) was **0 on
every row of every run** in this section, including overloaded steps.

### Balance read cost (`make bench-balance`)

| entries on the account | `ledger.Balance` (derived) | `ledger.StoredBalance` |
|---:|---:|---:|
| 1,000 | 0.18 ms | 0.11 ms |
| 10,000 | 0.77 ms | 0.11 ms |
| 100,000 | 7.0 ms | 0.12 ms |
| 1,000,000 | 28.7 ms | 0.12 ms |

The stored read is one network round trip at any history size; the spending path never sums.

### Spread across 1,000 accounts

| target rps | main achieved | main p50 / p99 ms | p08 achieved | p08 p50 / p99 ms | p08 non-202 |
|---:|---:|---:|---:|---:|---|
| 100 | 100 | 3.52 / 12.55 | 100 | 3.08 / 13.06 | 0 |
| 250 | 250 | 2.31 / 14.36 | 250 | 2.04 / 8.25 | 0 |
| 500 | 500 | 1.76 / 13.25 | 500 | 1.46 / 31.74 | 0 |
| 1,000 | 725 (6,475 dropped) | 1,768 / 4,173 | 1,000 | 1.50 / 96.44 | 0 |
| 1,500 | 632 (21,945 dropped) | 4,196 / 10,302 | 1,371 (2,117 dropped) | 365 / 2,023 | 0 |
| 2,000 | 309 (46,012 dropped) | 11,570 / 21,625 | 873 (29,625 dropped) | 4,464 / 5,435 | 4,733×500, 5,426×503 |

Two earlier p08 runs the same morning gave 18.2 and 16.7 ms p99 at the 500 step (main 13.25):
the p99 at 500 is noisy here and **worse than main in every run** (+26 % to +140 %), while
p50 is better and the knee moved from <1,000 to ~1,400. At 2,000 the server answers within
its 5 s deadline instead of after 20 s, but the 500s are the deadline expiring in the auth,
rate-limit and account lookups while waiting for a pool connection (only the transfer path
maps a timeout to 503).

### One hot account

| target rps | main 202s / dropped | main p99 ms | p08 202s (per s) | p08 503 | p08 p50 / p99 ms |
|---:|---:|---:|---:|---:|---:|
| 100 | 3,000 / 0 | 352 | 3,000 (100) | 0 | 3.23 / 11.66 |
| 250 | 7,501 / 0 | 546 | 7,500 (250) | 0 | 2.08 / 10.42 |
| 500 | 12,080 / 2,921 | 7,186 | 15,001 (500) | 0 | 1.45 / 17.76 |
| 1,000 | 9,545 / 20,456 | 21,139 | 29,946 (998) | 55 | 1.95 / 105.12 |
| 1,500 | 5,048 / 35,427 | 35,612 | 36,357 (1,212) | 8,644 | 101 / 159 |
| 2,000 | 2,587 / 49,995 | 53,959 | 21,497 (717) | 38,503 | 106 / 386 |

Main's non-202s at 1,500/2,000 were client timeouts (status 0) after 35–55 s. p08 drops no
arrival from 100 to 2,000, every non-202 is `503 account_busy`, and the slowest response in
the whole hot sweep was 569 ms.

### One hot account with 1,000,000 prior entries

| target rps | 202s (per s) | 503 | dropped | p50 / p99 ms |
|---:|---:|---:|---:|---:|
| 500 | 15,000 (500) | 0 | 0 | 1.46 / 25.0 |
| 1,000 | 29,772 (992) | 229 | 0 | 1.55 / 123 |
| 1,500 | 29,030 (968) | 15,971 | 0 | 103 / 634 |
| 2,000 | 13,624 (454) | 32,346 | 14,030 | 108 / 3,739 |
| 3,000 | 2,501 (83) | 52,192 | 35,309 | 1,272 / 3,728 |

Up to the ceiling the million entries change nothing (998 vs 992 accepted/s at 1,000). Above
it, accepted throughput falls with offered load in both runs (and more here, at 12 % battery):
see "what the numbers say" below. An earlier run that started straight after the bulk seed
measured autovacuum and checkpoint I/O instead, so the seed now vacuums and checkpoints first.

### Mixed: hot account at RATE plus spread at 300 req/s

| hot target | main hot 202s | main spread p99 ms (dropped) | p08 hot 202s (per s) / 503 | p08 spread p50 / p99 ms | p08 spread errors |
|---:|---:|---:|---:|---:|---:|
| 500 | 10,023 | 8,974 (2,899) | 15,001 (500) / 0 | 1.52 / 11.09 | 0 |
| 1,000 | 7,755 | 23,470 (6,260) | 24,709 (824) / 5,292 | 5.36 / 52.69 | 0 |
| 2,000 | 2,089 | 40,167 (6,960 + 900 timeouts) | 5,822 (194) / 54,179 | 138 / 538 | 0 |

On `main` one hot account takes spread traffic down with it (9–40 s p99, dropped arrivals,
timeouts). On p08 spread traffic is never refused and never dropped, but at 2x the hot
ceiling its p99 is 538 ms, not under 20 ms.

### Targets

| Check | Target | Result |
|---|---|---|
| Hot ceiling (highest step all 202, 0 dropped) | ≥ 1,200 req/s | **Missed as defined**: 500 is the last all-202 step. Accepted throughput 998/s at 1,000 (55 shed) and 1,212/s at 1,500; 3–4x same-session main (~330) |
| Hot with 1M entries | within 10 % of 0 history | **Met** up to the ceiling (992 vs 998/s at 1,000) |
| Mixed: spread p99 with hot ≥ 2x ceiling | < 20 ms, 0 spread errors | **Missed** on latency (538 ms), met on errors (0) |
| Spread p99 at 500 | ≤ +20 % of 9.08 ms | **Missed**: 16.7–31.7 ms over three runs; main 13.25 ms same session |
| Spread knee | ≥ ~1,100 req/s | **Met**: ~1,400 (main same session < 1,000) |
| Overload behaviour | 503/429, not 500; none slower than `requestTimeout` | **Met on the hot account** (all 503, max 569 ms). **Missed under spread overload**: 500s from auth/lookup deadlines, max 6.0 s at 2,000 |
| Balance drift after every run | 0 | **Met** |

### What the numbers say

1. **History no longer matters.** The stored balance read is 0.12 ms at 1M entries and the
   hot account accepts the same rate at 1M entries as at none.
2. **The per-account ceiling is now round trips, not the database.** A single-row
   `UPDATE` + commit inside the Postgres container runs at ~9,500/s (`pgbench`, 1 client),
   so the row lock is held ~0.1 ms by the database itself. The API holds it from the
   balance `UPDATE` until its `COMMIT` returns: about 1.5 host-to-container round trips
   plus the WAL flush, ~1 ms here. Removing `ledger.Record`'s savepoint (its `RELEASE`
   ran while the lock was held) took the 1,000 step from 86 % to 99.8 % accepted and the
   1,500 step from 565 to 1,212 accepted/s in back-to-back runs.
3. **Above the ceiling, refusals still cost the pool.** Every refused request has already
   done the API-key lookup and the rate-limit upsert, and the gate admits it after the
   account lookup, so at 2,000 req/s ~1,300 refusals/s still make three pooled round trips
   each, and the four admitted hot creations hold four of the ten connections. That is
   why accepted hot throughput falls past the ceiling (1,212/s at 1,500, 717/s at 2,000)
   and why spread p99 rises to 538 ms in the mixed run: spread requests queue for the
   pool behind work that will be refused. `pg_stat_activity` sampled during the mixed
   1,000 step showed most backends `idle`/`ClientRead` and a few `Lock:transactionid`
   (the balance row) and `LWLock:WALWrite`, not a database-side pile-up.
4. **Next levers**, in order: take the API-key lookup and rate limit off the money
   database (option 4, its own phase), so a refusal costs no pooled round trip; a
   bounded wait for a pool connection; then a bigger pool. Hot-account sharding or
   batching (option 6) only once the per-account ceiling itself is the limit.

## Phase 09: cards and card accounts (`28de6e5`, 2026-10-17)

Not a hot-path phase: the transfer path is unchanged; the phase adds migration 16 and the
card endpoints. Regression check only, same machine, pool 10, 30 s steps, **on battery
(43 % → 38 %), load average 3–6**. `main` (`91c1722`) was measured in the same session,
alternating with this branch. `balance_drift` was 0 on every step of every run.

```sh
MODES="spread hot" RATES="500 1000" make bench-http      # run 1, both branches
MODES=hot RATES="500 1000" make bench-http               # run 2, both branches
MODES=hot RATES="1000" make bench-http                   # run 3, both branches
```

### Spread across 1,000 accounts (run 1)

| target rps | main achieved | main p50 / p99 ms | p09 achieved | p09 p50 / p99 ms | non-202 (main / p09) |
|---:|---:|---:|---:|---:|---:|
| 500 | 500 | 1.45 / 18.92 | 500 | 1.41 / 6.16 | 0 / 0 |
| 1,000 | 1,000 | 1.16 / 47.67 | 1,000 | 1.18 / 41.43 | 0 / 0 |

### One hot account

| target rps | run | main 202s / 503 | main p50 / p99 ms | p09 202s / 503 | p09 p50 / p99 ms |
|---:|---:|---:|---:|---:|---:|
| 500 | 1 | 15,001 / 0 | 1.50 / 9.22 | 15,001 / 0 | 1.45 / 37.0 |
| 500 | 2 | 15,001 / 0 | 1.48 / 29.47 | 15,001 / 0 | 1.46 / 8.51 |
| 1,000 | 1 | 30,001 / 0 | 1.11 / 59.76 | 29,898 / 102 | 1.21 / 89.94 |
| 1,000 | 2 | 30,001 / 0 | 1.16 / 74.73 | 29,878 / 123 | 1.14 / 95.99 |
| 1,000 | 3 | 29,755 / 246 | 1.13 / 116.62 | 30,001 / 0 | 1.14 / 52.5 |

No regression. Spread p99 at 500 is 6.16 ms (main 18.92 ms, same session; rule: ≤ +20 %).
The hot 1,000 step sits at the per-account ceiling, so whether a run sheds 0 or a few
hundred requests (≤ 0.8 %) flips between runs on either branch: run 3 reversed runs 1–2.
Hot p99 at 500 swings 8.5–37 ms between runs of the same binary. No 1,500+ steps were run,
so this section makes no ceiling claim beyond "both branches accept ~1,000/s".

## Phase 10: card network simulator (2026-10-17)

No benchmark run: phase 10 adds `cmd/mock-network` and `internal/cardnetwork`, a test
counterparty, and changes no API path. The regression run is deferred to phase 11, which
adds the authorization hot path.

## Phase 11: card authorization (2026-10-17)

`POST /network/authorizations` is the new hot path. Same machine, pool 10, 30 s steps, **on
battery (28 % → 17 %), load average ~1.6 before the runs**, only the Postgres container
running. `benchseed -cards` issues one card per source account and one on the hot account
(limits no run reaches, no blocked merchants), so every tap is approved on funds; each row
reports `declined_after` and it was **0 on every auth step**. Requests are signed by k6
exactly as the contract says, with a throwaway `NETWORK_SECRET` generated by the script.
`balance_drift` was 0 on every step of every run. `main` (`5b66854`) was measured in the
same session for the transfer regression, alternating with this branch.

```sh
MODES="auth-spread auth-hot" RATES="250 500 1000 1500" make bench-http   # run 1
MODES=auth-spread RATES="1000 1500" make bench-http                     # run 2 (see below)
MODES="spread hot" RATES="500 1000" make bench-http                     # run 1, both branches
MODES=spread RATES="500 1000" make bench-http                           # runs 2-3, both branches
```

Run 1's auth-spread 1,500 step is discarded: the laptop idle-slept for 12.5 minutes in the
middle of it (`pmset -g log`: "Entering Sleep state due to 'Idle Sleep'" 11:47:30, wake
12:00:00), which shows up as a 748 s "max latency". Later runs were wrapped in
`caffeinate -i`.

### Authorizations, spread across 1,001 cards

| target rps | run | achieved | p50 ms | p95 ms | p99 ms | max ms | non-200 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 250 | 1 | 250 | 2.21 | 2.93 | 4.31 | 24.4 | 0 |
| 500 | 1 | 500 | 1.47 | 1.89 | **3.17** | 30.6 | 0 |
| 1,000 | 1 | 994 | 0.84 | 4.05 | **57.41** | 263.6 | 0 |
| 1,000 | 2 | 999 | 0.78 | 1.57 | **33.90** | 92.6 | 0 |
| 1,500 | 2 | 1,500 | 0.76 | 13.96 | 100.98 | 152.9 | 0 |

### Authorizations on one hot card

| target rps | achieved | 200s (per s) | 503 | p50 ms | p95 ms | p99 ms | max ms |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 250 | 250 | 7,501 (250) | 0 | 2.22 | 5.05 | 72.0 | 203.8 |
| 500 | 500 | 15,000 (500) | 0 | 1.47 | 2.01 | 7.75 | 48.7 |
| 1,000 | 1,000 | 29,984 (999) | 17 | 0.82 | 5.89 | 76.18 | 145.6 |
| 1,500 | 1,500 | 44,873 (1,496) | 127 | 0.74 | 37.12 | 93.48 | 160.9 |

The hot card answers ~1,500 authorizations/s, the same order as the phase 08 transfer hot
ceiling (1,212 accepted/s at 1,500): every 503 is the per-account gate or lock timeout
refusing fast, which the network treats as no answer (retry, then stand-in). No arrival was
dropped and the slowest answer of the hot sweep was 204 ms, inside the 1 s deadline.

### Transfer regression (spread and hot, `main` vs phase 11 in the same session)

| step | run | main p50 / p99 ms | p11 p50 / p99 ms | non-202 (main / p11) |
|---|---:|---:|---:|---:|
| spread 500 | 1 | 1.52 / 3.45 | 1.49 / 4.51 | 0 / 0 |
| spread 500 | 2 | 1.55 / 7.28 | 1.53 / 8.46 | 0 / 0 |
| spread 500 | 3 | 1.53 / 4.00 | 1.52 / 5.49 | 0 / 0 |
| spread 1,000 | 1 | 1.11 / 26.11 | 1.20 / 60.62 | 0 / 0 |
| spread 1,000 | 2 | 1.12 / 12.01 | 1.19 / 14.62 | 0 / 0 |
| spread 1,000 | 3 | 1.12 / 14.09 | 1.18 / 28.34 | 0 / 0 |
| hot 500 | 1 | 1.51 / 6.63 | 1.52 / 9.46 | 0 / 0 |
| hot 1,000 | 1 | 1.11 / 38.58 | 1.20 / 32.45 | 0 / 0 |

### Targets

| Check | Target | Result |
|---|---|---|
| auth-spread p99 at 500 req/s, 0 non-200 | < 100 ms | **Met**: 3.17 ms, 0 non-200 |
| auth-spread p99 at 1,000 req/s, 0 non-200 | < 100 ms | **Met**: 57.4 and 33.9 ms in two runs, 0 non-200 |
| auth-hot ceiling | record it; ~1,000+ expected | ~1,500 answered/s (0.3 % refused with 503 at 1,500); strict all-200 ceiling 500 |
| Transfer regression | within noise of phase 8 / same-session main | **Missed at p99 as the rule is written**: spread 500 p99 is +1.0 to +1.5 ms over main in all three alternating runs (+16 % to +37 %); p50 equal or lower; hot no worse |
| Balance drift after every step | 0 | **Met** |

### What the numbers say

1. **An authorization is cheaper than a transfer.** No API-key lookup and no rate-limit
   upsert (the network is authenticated by its signature, verified in memory), so a tap is
   a card read plus four round trips in its transaction (BEGIN, one batch with the row, the
   lock timeout, the event and the daily counter, the hold batch, COMMIT). p50 is 0.8 ms at
   1,000/s against 1.2 ms for a transfer.
2. **The hot card is bound by the same row locks as the hot account,** now two of them: the
   daily counter row (taken in the first batch) and the balance rows (taken in the hold
   batch), both held until commit. The counter lock is held for one round trip longer than
   the balance lock, which is why the event is queued before the counter and not after.
3. **The transfer p99 at 500 is consistently ~1 ms higher than main.** The only change on
   that path is the `LEFT JOIN card_accounts` in the account validation (one unique-index
   probe), and the fixture is larger (1,001 held accounts and cards). The difference is
   inside the 3.5–8.5 ms run-to-run swing of either branch but appears in every pairing,
   so it is reported as a miss rather than called noise. It was not isolated further in
   this session (battery at 17 %).
4. **The daily-limit and insufficient-funds paths were not loaded** (every bench tap is
   approved). They cost a rollback plus one more statement; the concurrency tests, not
   this sweep, show them correct.

## Phase 12: reversals, expiry and incremental holds (2026-10-17)

Every approval's first batch now also takes a per-`auth_id` advisory lock and reads
`pending_reversals` (same round trip). Same machine, pool 10, 30 s steps, **on battery
(13 % → 9 %), load average ~2**, only the Postgres container running, wrapped in
`caffeinate -i`. `main` (`341624e`) was measured in the same session: run 1 branch then main,
run 2 main then branch. `balance_drift` 0 and `declined_after` 0 on every step.

```sh
make bench-expiry                                              # BenchmarkExpireBatch, 3 runs
MODES="auth-spread auth-hot" RATES="500 1000" make bench-http  # run 1, both
MODES="spread" RATES="500 1000" make bench-http                # run 1, both
MODES="auth-spread spread" RATES="500 1000" make bench-http    # run 2, both
```

Main's run-2 auth-spread steps are discarded: the laptop slept in the middle of them
(max latency 714 s and 964 s, 14 achieved/s), even under `caffeinate -i`, at 9 % battery.

### Authorizations (p50 / p99 ms, every step 0 non-200, 0 dropped)

| step | run | main | p12 |
|---|---:|---:|---:|
| auth-spread 500 | 1 | 1.58 / 2.97 | 1.57 / 15.56 |
| auth-spread 500 | 2 | (slept) | 1.60 / 2.73 |
| auth-spread 1,000 | 1 | 0.80 / 3.79 | 0.80 / 4.21 |
| auth-spread 1,000 | 2 | (slept) | 0.77 / 10.53 |
| auth-hot 500 | 1 | 1.63 / 4.19 | 1.62 / 3.25 |
| auth-hot 1,000 | 1 | 0.81 / 6.21 | 0.81 / 6.59 |

### Transfer regression (p50 / p99 ms, every step all 202, 0 dropped)

| step | run | main | p12 |
|---|---:|---:|---:|
| spread 500 | 1 | 1.49 / 2.82 | 1.49 / 3.20 |
| spread 500 | 2 | 1.45 / 6.83 | 1.48 / 3.49 |
| spread 1,000 | 1 | 1.14 / 10.27 | 1.23 / 7.92 |
| spread 1,000 | 2 | 1.24 / 17.22 | 1.19 / 28.95 |

### Expiry (`make bench-expiry`)

| holds | run 1 | run 2 | run 3 |
|---:|---:|---:|---:|
| 10,000 on one account | 7.18 s (1,393/s) | 7.13 s (1,402/s) | 7.07 s (1,415/s) |

One runner, one transaction per hold (~0.7 ms: BEGIN, claim batch, row lock, release
batch, ledger batch, COMMIT), every release on the same two balance rows. ~120 M holds/day
per runner; more runners help only across accounts.

### Targets

| Check | Target | Result |
|---|---|---|
| auth-spread p99 at 500 and 1,000 req/s | < 100 ms | **Met**: 15.6 / 2.7 ms at 500, 4.2 / 10.5 ms at 1,000, 0 non-200 |
| No worse than same-session main beyond noise | | **Met on p50** (equal on every pairing). p99 at auth-spread 500 run 1 was 15.6 ms vs 2.97 ms: the branch's first step of the session; its run 2 gave 2.73 ms. Not reproduced, so reported, not explained |
| Transfer spread p99 at 500 (regression rule) | ≤ +20 % of main | **Met**: +13 % in run 1 (3.20 vs 2.82), −49 % in run 2 (3.49 vs 6.83) |
| Balance drift after every step | 0 | **Met** |

### What the numbers say

1. **The advisory lock and pending-reversal read ride in the existing first batch**, so
   an approval still makes the same number of round trips; p50 is unchanged to 0.01-0.03 ms
   at every step. The lock is on the `auth_id`, which only duplicates and reversals of the
   same authorization share, so it adds no contention on the hot card.
2. **The p99 swings are the run-to-run noise of this laptop** (main's own spread 1,000 p99
   went 10.3 → 17.2 ms between runs; the branch's 7.9 → 29.0 ms). Reversals, incrementals
   and expiry are not loaded by this sweep; their correctness is shown by the concurrency
   tests, and the expirer's throughput by `BenchmarkExpireBatch`.

## Regression rule

Each phase's PR must include a re-run of `make bench-http` (same rates) and paste the two
tables. A p99 regression of more than 20 % at the 500 req/s spread step, or a lower hot
ceiling, blocks merge unless the PR states why the trade is worth it.
