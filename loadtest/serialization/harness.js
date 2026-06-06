'use strict'
// Load harness reproducing the Manifold bet-path serialization story.
//
// Modes:
//   baseline : queue keyed on contractId  + contract-row write INSIDE the serializable txn
//              (== production today). All answers serialize through one queue slot.
//   naive    : queue keyed on contractId:answerId + contract-row STILL inside the txn.
//              (== the trap I flagged: answers now run concurrently, but all hammer the
//               shared contracts row -> 40001 serialization_failure retry storm.)
//   fixed    : queue keyed on contractId:answerId + contract-row aggregates DEFERRED out
//              of the txn (Change I). Independent answers touch disjoint rows -> real
//              parallelism, no retries.
//
// The txn body mirrors backend/api/src/place-bet.ts pgTrans.multi(...): per-user balance,
// bet insert, per-answer pool, per-(user,answer) metric, and (baseline/naive) the shared
// contracts aggregate row. Retry wrapper mirrors backend/shared/src/transact-with-retries.ts.

const { Pool } = require('pg')
const crypto = require('crypto')
const { createFnQueue } = require('./fn-queue')

const env = (k, d) => (process.env[k] !== undefined ? process.env[k] : d)
const MODE = env('MODE', 'baseline')
const VUS = parseInt(env('VUS', '200'), 10)
const ANSWERS = parseInt(env('ANSWERS', '20'), 10)
const DURATION_MS = parseInt(env('DURATION_MS', '15000'), 10)
const TXN_LATENCY_MS = parseFloat(env('TXN_LATENCY_MS', '6')) // models remote DB RTT + fsync
const POOL_MAX = parseInt(env('POOL_MAX', '12'), 10)
const FLUSH_MS = parseInt(env('FLUSH_MS', '200'), 10)
const CONTRACT = 'C1'

const pool = new Pool({
  host: env('PGHOST', '127.0.0.1'),
  port: parseInt(env('PGPORT', '5432'), 10),
  user: env('PGUSER', 'loadtest'),
  password: env('PGPASSWORD', 'loadtest'),
  database: env('PGDATABASE', 'manifold_load'),
  max: POOL_MAX,
})

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const randInt = (n) => Math.floor(Math.random() * n)

// ---- transact-with-retries.ts mirror -------------------------------------
async function withRetries(fn) {
  const maxAttempts = 3
  let attempt = 0
  const client = await pool.connect()
  try {
    while (true) {
      attempt++
      try {
        return await fn(client)
      } catch (error) {
        const retryable = error.code === '40001' || error.code === '40P01'
        if (retryable) metrics.retries++
        if (!retryable || attempt >= maxAttempts) throw error
        await sleep(Math.min(100 * Math.pow(2, attempt - 1), 5000))
      }
    }
  } finally {
    client.release()
  }
}

// ---- deferred contract-aggregate buffer (Change I, fixed mode) -----------
const aggBuffer = new Map() // contractId -> { vol, lastTs }
let flushCount = 0
function accumulate(contractId, amount, ts) {
  const cur = aggBuffer.get(contractId) || { vol: 0, lastTs: 0 }
  cur.vol += amount
  cur.lastTs = Math.max(cur.lastTs, ts)
  aggBuffer.set(contractId, cur)
}
async function flushAggregates() {
  if (aggBuffer.size === 0) return
  const entries = [...aggBuffer.entries()]
  aggBuffer.clear()
  const client = await pool.connect()
  try {
    // READ COMMITTED atomic increment; concurrent flushes for the same row just
    // briefly row-lock-serialize (no 40001). Recomputable -> synchronous_commit off is safe.
    await client.query('begin isolation level read committed')
    await client.query('set local synchronous_commit = off')
    for (const [cid, { vol, lastTs }] of entries) {
      await client.query(
        `update contracts set data = data
           || jsonb_build_object(
                'volume', (data->>'volume')::numeric + $2,
                'lastBetTime', greatest((data->>'lastBetTime')::bigint, $3))
         where id = $1`,
        [cid, vol, lastTs]
      )
    }
    await client.query('commit')
    flushCount++
  } catch (e) {
    await client.query('rollback').catch(() => {})
    // put deltas back so they're not lost
    for (const [cid, d] of entries) accumulate(cid, d.vol, d.lastTs)
  } finally {
    client.release()
  }
}

// ---- bet write path (mirrors pgTrans.multi statements) -------------------
async function betTxn(client, m) {
  await client.query('begin isolation level serializable')
  try {
    // reads done inside the txn (mirror getUserBalancesAndMetrics + pool read)
    await client.query('select data from answers where id = $1', [m.A])
    await client.query('select balance from users where id = $1', [m.U])
    // model network RTT + remote fsync + compute, holding the txn open
    if (TXN_LATENCY_MS > 0) await sleep(TXN_LATENCY_MS)

    // --- disjoint-row writes (all modes) ---
    await client.query('update users set balance = balance - $2 where id = $1', [m.U, m.M])
    await client.query(
      `insert into contract_bets (bet_id, contract_id, answer_id, user_id, data)
       values ($1,$2,$3,$4,$5)`,
      [m.betId, CONTRACT, m.A, m.U, { amount: m.M, ts: m.ts }]
    )
    await client.query(
      `update answers set data = data
         || jsonb_build_object('poolYes', (data->>'poolYes')::numeric + $2,
                               'v', (data->>'v')::numeric + 1)
       where id = $1`,
      [m.A, m.M]
    )
    await client.query(
      `insert into contract_metrics (user_id, contract_id, answer_id, data)
       values ($1,$2,$3,$4)
       on conflict (user_id, contract_id, answer_id)
       do update set data = jsonb_build_object('n', (contract_metrics.data->>'n')::int + 1)`,
      [m.U, CONTRACT, m.A, { n: 1 }]
    )

    // --- shared contracts-row write: the cross-answer contention point ---
    if (MODE !== 'fixed') {
      await client.query(
        `update contracts set data = data
           || jsonb_build_object('volume', (data->>'volume')::numeric + $2,
                                 'lastBetTime', $3::bigint)
         where id = $1`,
        [CONTRACT, m.M, m.ts]
      )
    }

    await client.query('commit')
    if (MODE === 'fixed') accumulate(CONTRACT, m.M, m.ts) // defer instead
  } catch (e) {
    await client.query('rollback').catch(() => {})
    throw e
  }
}

const betsQueue = createFnQueue({ name: 'Bets' })
function placeBet(m) {
  const deps =
    MODE === 'baseline'
      ? [`c:${CONTRACT}`, `u:${m.U}`]
      : [`p:${CONTRACT}:${m.A}`, `u:${m.U}`]
  return betsQueue.enqueueFn(() => withRetries((client) => betTxn(client, m)), deps)
}

// ---- metrics -------------------------------------------------------------
const metrics = { ok: 0, q503: 0, errOther: 0, retries: 0, lat: [], qDepthMax: 0 }

// ---- worker --------------------------------------------------------------
async function worker(userId, deadline) {
  while (Date.now() < deadline) {
    const A = `A${randInt(ANSWERS)}`
    const start = Date.now()
    try {
      await placeBet({ A, U: userId, M: 1, ts: Date.now(), betId: crypto.randomUUID() })
      metrics.ok++
      metrics.lat.push(Date.now() - start)
    } catch (e) {
      if (e.code === 503) metrics.q503++
      else {
        metrics.errOther++
        if (metrics.errOther <= 3) console.error('  err:', e.code, e.message)
      }
    }
  }
}

function pct(arr, p) {
  if (arr.length === 0) return 0
  const s = [...arr].sort((a, b) => a - b)
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]
}

async function setup() {
  const fs = require('fs')
  const schema = fs.readFileSync(require('path').join(__dirname, 'schema.sql'), 'utf8')
  const c = await pool.connect()
  try {
    await c.query(schema)
    await c.query(
      `insert into contracts (id, data) values ($1, $2)`,
      [CONTRACT, { volume: 0, uniqueBettorCount: 0, lastBetTime: 0 }]
    )
    for (let i = 0; i < ANSWERS; i++) {
      await c.query(`insert into answers (id, contract_id, index, data) values ($1,$2,$3,$4)`,
        [`A${i}`, CONTRACT, i, { poolYes: 1000, poolNo: 1000, prob: 0.5, v: 0 }])
    }
    for (let i = 0; i < VUS; i++) {
      await c.query(`insert into users (id, balance) values ($1, $2)`, [`U${i}`, 1e12])
    }
    console.log(`setup: contract + ${ANSWERS} answers + ${VUS} users`)
  } finally {
    c.release()
  }
}

async function run() {
  const qSampler = setInterval(() => {
    metrics.qDepthMax = Math.max(metrics.qDepthMax, betsQueue._state.fnQueue.length)
  }, 50)
  const flusher = MODE === 'fixed' ? setInterval(flushAggregates, FLUSH_MS) : null

  const t0 = Date.now()
  const deadline = t0 + DURATION_MS
  const workers = []
  for (let i = 0; i < VUS; i++) workers.push(worker(`U${i}`, deadline))
  await Promise.all(workers)
  const elapsed = (Date.now() - t0) / 1000

  clearInterval(qSampler)
  if (flusher) {
    clearInterval(flusher)
    await flushAggregates()
  }

  const total = metrics.ok + metrics.q503 + metrics.errOther
  console.log(`\n=== MODE=${MODE} VUS=${VUS} ANSWERS=${ANSWERS} txnLatency=${TXN_LATENCY_MS}ms poolMax=${POOL_MAX} dur=${elapsed.toFixed(1)}s ===`)
  console.log(`throughput:      ${(metrics.ok / elapsed).toFixed(0)} bets/sec  (${metrics.ok} ok)`)
  console.log(`attempts total:  ${total}`)
  console.log(`queue 503s:      ${metrics.q503}  (${((100 * metrics.q503) / Math.max(1, total)).toFixed(1)}%)`)
  console.log(`other errors:    ${metrics.errOther}`)
  console.log(`40001 retries:   ${metrics.retries}`)
  console.log(`latency ok ms:   p50=${pct(metrics.lat, 50)}  p95=${pct(metrics.lat, 95)}  p99=${pct(metrics.lat, 99)}  max=${pct(metrics.lat, 100)}`)
  console.log(`max queue depth: ${metrics.qDepthMax}`)
  if (MODE === 'fixed') console.log(`agg flushes:     ${flushCount}`)
}

async function main() {
  if (process.argv.includes('setup')) {
    await setup()
  } else {
    await run()
  }
  await pool.end()
}
main().catch((e) => {
  console.error(e)
  process.exit(1)
})
