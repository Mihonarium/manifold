# Bet-path serialization load harness

Reproduces, against a **real** local Postgres, the contention that makes hot
multiple-choice markets collapse — and validates the per-answer parallelization
in this PR. It reuses the production `fn-queue` scheduling logic (vendored in
`fn-queue.js`) and mirrors the `place-bet.ts` write path's contention footprint
(per-user balance, bet insert, per-answer pool, per-(user,answer) metric, and
the shared `contracts` row).

It is **not** the full CPMM math — it isolates the row-level contention, which
is what serializes bets.

## Modes

- `baseline` — queue keyed on `contractId`, contract-row write **inside** the
  serializable txn (production today). All answers serialize.
- `naive` — queue keyed on `contractId:answerId`, contract row **still** in the
  txn. Demonstrates the trap: answers run concurrently but hammer the shared
  contract row → `40001` retry storm (worse than baseline).
- `fixed` — per-answer key **and** contract-row aggregates deferred out of the
  txn (this PR). Independent answers touch disjoint rows → real parallelism.

## Run

```bash
# 1. point at a local Postgres (defaults: 127.0.0.1:5432 loadtest/loadtest)
createdb manifold_load   # or use your own; set PG* env vars
npm install              # installs `pg`

# 2. seed: one contract, N answers, V users
VUS=300 ANSWERS=20 node harness.js setup

# 3. run a mode, pinned to 2 cores (don't hog the box)
MODE=baseline VUS=1500 ANSWERS=20 TXN_LATENCY_MS=8 ./run.sh   # CORES=0,1 by default
node harness.js setup >/dev/null   # reset between modes
MODE=fixed    VUS=1500 ANSWERS=20 TXN_LATENCY_MS=8 ./run.sh
```

`TXN_LATENCY_MS` models remote-DB round-trip + fsync (the time the txn holds its
locks). Tune to match your real per-bet latency.

## Result (16-core box, harness pinned to 2 cores, PG to 2 cores)

20-answer market, 1500 concurrent bettors, `TXN_LATENCY_MS=8`:

| mode | throughput | p50 latency | 503 failures |
|------|-----------:|------------:|-------------:|
| baseline | 75/s | 9,787 ms | **45.6%** |
| fixed    | 788/s | 1,582 ms | **0%** |

At a moderate load (200 VUs) `fixed` is ~12.7× baseline throughput at ~20× lower
latency. `naive` is *worse* than baseline (66/s, 2,553 retries) — proof the
contract-row deferral, not just the queue-key split, is what matters.
