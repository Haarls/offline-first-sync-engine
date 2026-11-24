# offline-first-sync-engine

Deterministic bidirectional sync engine bridging local SQLite nodes (POS terminals, desktop clients) with PostgreSQL / Neon.
Guarantees convergence under concurrent offline edits via Last-Write-Wins tie-breaking, idempotent outbox replay, and immutable multi-currency snapshots.

## Sync Flow

```mermaid
sequenceDiagram
    participant SQLite as LocalSyncNode (SQLite WAL)
    participant Outbox as Outbox Table
    participant PG as PostgresRemote (Neon / PG)

    SQLite->>Outbox: Atomic TX: upsert record + append UUID mutation
    Outbox->>PG: push(batch) with exponential backoff + jitter
    PG->>PG: Dedup idempotency_key + LWW upsert (updated_at, client_id)
    PG-->>Outbox: ackedKeys -> delete acknowledged rows
    SQLite->>PG: pull(afterSeq)
    PG-->>SQLite: Apply records newer than local state + advance cursor
```

## Why It's Built This Way

- **Transactional Outbox in SQLite WAL**: Every `put()` and `remove()` updates materialized local state and appends a queued event inside a single SQLite transaction. Crash or power loss mid-operation cannot leave local reads ahead of or behind the outbox.
- **Deterministic LWW Convergence (`COLLATE "C"`)**: Conflict resolution compares UTC millisecond timestamps first (`updated_at`) and breaks exact ties using bytewise code-unit ordering on `client_id`. PostgreSQL `sync_records.client_id` explicitly uses `COLLATE "C"` so server-side SQL comparisons match JavaScript string comparisons regardless of OS locale.
- **At-Least-Once Push + Idempotent Apply**: Outbox entries are deleted locally only after receiving `ackedKeys` from the remote store. If a socket drops after the remote transaction commits, re-pushing the batch hits `sync_applied_mutations (idempotency_key)` with `ON CONFLICT DO NOTHING` instead of re-applying stale mutations.
- **Equal-Jitter Exponential Backoff**: Transient network failures back off over `[ceiling / 2, ceiling]` up to `maxDelayMs`, preventing reconnect storms when multiple POS registers regain connectivity simultaneously.
- **Minor-Unit Multi-Currency Freezing**: `freezeCurrencyTx()` converts via integer cents (`Math.round(baseCents * rate) / 100`) and freezes `{ baseAmount, quoteCurrency, exchangeRate, convertedAmount, capturedAt }` inside the transaction payload so future exchange rate changes never alter historical ledger entries.

## Quick Start (3 Steps)

1. Install dependencies:
   ```powershell
   npm install
   ```

2. Run strict typecheck, build, and test suite (zero external services required):
   ```powershell
   npm run typecheck
   npm run build
   npm test
   ```

3. Connect to Neon Serverless PostgreSQL (or start local Postgres via `docker compose up -d`):
   ```powershell
   Copy-Item .env.example .env
   # Set DATABASE_URL="postgresql://user:pass@ep-host.aws.neon.tech/neondb?sslmode=require"
   ```

## Programmatic Usage

```ts
import { LocalSyncNode, PostgresRemote, freezeCurrencyTx } from './src/index.js';

const local = new LocalSyncNode({ dbPath: './terminal-01.db', clientId: 'pos-01' });
const remote = new PostgresRemote(process.env.DATABASE_URL!);
await remote.migrate();

const payment = freezeCurrencyTx({
  amountUsd: 24.5,
  quoteCurrency: 'EUR',
  exchangeRate: 0.92,
});

local.put('invoice', 'inv-2026-0001', { status: 'PAID', payment });

const stats = await local.sync(remote);
console.log(stats); // { pushed: 1, duplicatesSkipped: 0, pulled: 0, cursor: 1 }
```

## Author

Carlos Almonte (**Haarls**) — [haarls.dev](https://haarls.dev) · [carlos@haarls.dev](mailto:carlos@haarls.dev)
