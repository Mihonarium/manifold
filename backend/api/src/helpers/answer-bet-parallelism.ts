import { Contract } from 'common/contract'

// Feature flag. When enabled, single-answer bets on independent (non-sum-to-one)
// multiple-choice markets are keyed per-answer in the bets queue and their
// contract-row aggregate writes are deferred (see contract-aggregate-buffer),
// so independent answers execute in parallel instead of serializing on the
// shared contract row. Defaults off; ramp via env once validated by metrics.
export const PARALLEL_ANSWER_BETS = process.env.PARALLEL_ANSWER_BETS === 'true'

// Immutable per-contract metadata, used to choose the queue key *before* the
// contract is fetched. `mechanism` and `shouldAnswersSumToOne` are fixed at
// creation, so this cache never needs invalidation.
type ContractMeta = { mechanism: string; shouldAnswersSumToOne: boolean }
const metaCache = new Map<string, ContractMeta>()

export const cacheContractMeta = (contract: Contract) => {
  if (metaCache.has(contract.id)) return
  metaCache.set(contract.id, {
    mechanism: contract.mechanism,
    shouldAnswersSumToOne:
      'shouldAnswersSumToOne' in contract
        ? !!contract.shouldAnswersSumToOne
        : false,
  })
}

// True when an independent answer of this contract may be keyed/persisted on its
// own. Requires the flag, a known (cached) multi market, and that answers do not
// sum to one (sum-to-one answers are arbitrage-coupled and must stay coupled).
export const canParallelizeAnswer = (
  contractId: string,
  answerId: string | undefined
) => {
  if (!PARALLEL_ANSWER_BETS || !answerId) return false
  const meta = metaCache.get(contractId)
  return (
    !!meta && meta.mechanism === 'cpmm-multi-1' && !meta.shouldAnswersSumToOne
  )
}

// The queue dependency token for the pool a bet touches. Independent answers get
// their own token (parallel); binary / sum-to-one / unknown fall back to the
// coarse contract token (safe — the queue is only a performance heuristic, and
// the SERIALIZABLE transaction remains the correctness guard).
export const getPoolDepToken = (
  contractId: string,
  answerId: string | undefined
) =>
  canParallelizeAnswer(contractId, answerId)
    ? `${contractId}:${answerId}`
    : contractId
