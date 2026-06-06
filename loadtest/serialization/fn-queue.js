// Vendored from backend/shared/src/helpers/fn-queue.ts (TS->JS, verbatim semantics).
// Only change: APIError/log replaced with local stubs. The queue scheduling logic
// (dependency-conjunctive locking, 10s expiry -> 503) is identical to production.

const DEFAULT_QUEUE_TIME_LIMIT = 10_000

class QueueOverflowError extends Error {
  constructor(message) {
    super(message)
    this.code = 503
    this.name = 'QueueOverflowError'
  }
}

function remove(arr, pred) {
  for (let i = arr.length - 1; i >= 0; i--) if (pred(arr[i])) arr.splice(i, 1)
}

function createFnQueue(props) {
  const { timeout = DEFAULT_QUEUE_TIME_LIMIT, name = 'unnamed' } = props || {}

  const state = { fnQueue: [], activeItems: [] }
  const { fnQueue, activeItems } = state

  const enqueuePrivate = (fn, dependencies, first) =>
    new Promise((resolve, reject) => {
      const item = { fn, resolve, reject, dependencies, timestamp: Date.now() }
      if (first) fnQueue.unshift(item)
      else fnQueue.push(item)
      run()
    })

  const enqueueFn = (fn, dependencies) => enqueuePrivate(fn, dependencies, false)

  const spliceExpiredItems = (queue) => {
    const now = Date.now()
    const expiredBeforeIndex = queue.findIndex(
      (item) => now - item.timestamp < timeout
    )
    const expiredCount =
      expiredBeforeIndex === -1 ? queue.length : expiredBeforeIndex
    return queue.splice(0, expiredCount)
  }

  const runItem = async (item) => {
    const { fn, resolve, reject } = item
    activeItems.push(item)
    try {
      resolve(await fn())
    } catch (e) {
      reject(e)
    } finally {
      remove(activeItems, (i) => i === item)
      run()
    }
  }

  const run = () => {
    const expiredItems = spliceExpiredItems(fnQueue)
    for (const item of expiredItems) {
      item.reject(
        new QueueOverflowError(
          `High volume of requests (${fnQueue.length} requests in queue); please try again later.`
        )
      )
    }

    const cumulativeDependencies = new Set(
      activeItems.flatMap((item) => item.dependencies)
    )
    const toRun = []
    for (const item of fnQueue) {
      const { dependencies } = item
      if (!dependencies.some((d) => cumulativeDependencies.has(d))) {
        toRun.push(item)
      }
      dependencies.forEach((d) => cumulativeDependencies.add(d))
    }

    const runSet = new Set(toRun)
    remove(fnQueue, (item) => runSet.has(item))
    for (const item of toRun) runItem(item)
  }

  return { enqueueFn, _state: state }
}

module.exports = { createFnQueue, QueueOverflowError, DEFAULT_QUEUE_TIME_LIMIT }
