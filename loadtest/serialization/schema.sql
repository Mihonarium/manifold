-- Minimal schema mirroring the rows a Manifold bet touches.
-- Goal: faithfully reproduce the *contention footprint* of the real
-- place-bet write path (backend/api/src/place-bet.ts -> pgTrans.multi(...)),
-- not the full CPMM math.

drop table if exists contract_bets, contract_metrics, answers, contracts, users cascade;

-- users.balance is incremented atomically in the real code
-- (bulkIncrementBalancesQuery: `balance = balance + delta`). Per-user row.
create table users (
  id text primary key,
  balance numeric not null default 0
);

-- The single shared row per contract. In the real code every bet does a
-- read-modify-write of data.volume / data.uniqueBettorCount / data.lastBetTime
-- (updateDataQuery, absolute values) -> this is the cross-answer contention point.
create table contracts (
  id text primary key,
  data jsonb not null
);

-- Per-answer pool state for cpmm-multi-1. THIS is the per-answer strongly-consistent
-- state (separate row per answer) -> independent answers touch disjoint rows here.
create table answers (
  id text primary key,
  contract_id text not null,
  index int not null,
  data jsonb not null
);

-- One row per bet (unique id -> never conflicts).
create table contract_bets (
  bet_id text primary key,
  contract_id text not null,
  answer_id text,
  user_id text not null,
  data jsonb not null,
  created_time timestamptz not null default now()
);

-- Per (user, contract, answer) -> disjoint across answers / users.
create table contract_metrics (
  user_id text not null,
  contract_id text not null,
  answer_id text,
  data jsonb not null,
  primary key (user_id, contract_id, answer_id)
);

create index on contract_bets (contract_id);
create index on contract_bets (contract_id, answer_id);
create index on answers (contract_id);
