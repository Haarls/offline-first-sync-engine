import { Pool, type PoolConfig } from 'pg';
import {
  MutationSchema,
  type Mutation,
  type PullResult,
  type PushResult,
  type RemoteStore,
} from './types.js';

interface PgRecordRow {
  server_seq: string;
  entity: string;
  entity_id: string;
  client_id: string;
  payload: Record<string, unknown>;
  updated_at: Date;
  revision: number;
  deleted: boolean;
}

const TLS_MODES = new Set(['require', 'verify-ca', 'verify-full']);

export function buildPgPoolConfig(connectionString: string, max = 5): PoolConfig {
  const parsed = new URL(connectionString);
  const sslMode = parsed.searchParams.get('sslmode');
  const useTls = parsed.hostname.endsWith('.neon.tech') || (sslMode !== null && TLS_MODES.has(sslMode));

  // Strip sslmode from URL when enabling explicit TLS options so pg-connection-string does not override rejectUnauthorized.
  if (useTls && sslMode) parsed.searchParams.delete('sslmode');

  return {
    connectionString: parsed.toString(),
    max,
    idleTimeoutMillis: 10_000,
    connectionTimeoutMillis: 5_000,
    ...(useTls ? { ssl: { rejectUnauthorized: true } } : {}),
  };
}

export class PostgresRemote implements RemoteStore {
  private readonly pool: Pool;

  constructor(connectionStringOrPool: string | Pool) {
    this.pool =
      typeof connectionStringOrPool === 'string'
        ? new Pool(buildPgPoolConfig(connectionStringOrPool))
        : connectionStringOrPool;
  }

  async migrate(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS sync_applied_mutations (
        idempotency_key UUID PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS sync_records (
        server_seq BIGSERIAL NOT NULL,
        entity TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        client_id TEXT NOT NULL COLLATE "C",
        payload JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        revision INTEGER NOT NULL,
        deleted BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (entity, entity_id)
      );
      CREATE INDEX IF NOT EXISTS idx_sync_records_server_seq ON sync_records (server_seq);
    `);
  }

  async push(mutations: Mutation[]): Promise<PushResult> {
    if (mutations.length === 0) {
      return { ackedKeys: [], appliedCount: 0, duplicateCount: 0 };
    }

    const validated = mutations.map((m) => MutationSchema.parse(m));
    const client = await this.pool.connect();
    const ackedKeys: string[] = [];
    let appliedCount = 0;
    let duplicateCount = 0;

    try {
      await client.query('BEGIN');
      for (const m of validated) {
        const dedup = await client.query(
          `INSERT INTO sync_applied_mutations (idempotency_key) VALUES ($1)
           ON CONFLICT (idempotency_key) DO NOTHING`,
          [m.idempotencyKey],
        );

        ackedKeys.push(m.idempotencyKey);
        if (dedup.rowCount === 0) {
          duplicateCount++;
          continue;
        }

        await client.query(
          `INSERT INTO sync_records (entity, entity_id, client_id, payload, updated_at, revision, deleted)
           VALUES ($1, $2, $3, $4::jsonb, $5::timestamptz, $6, $7)
           ON CONFLICT (entity, entity_id) DO UPDATE SET
             server_seq = nextval(pg_get_serial_sequence('sync_records', 'server_seq')),
             client_id = EXCLUDED.client_id,
             payload = EXCLUDED.payload,
             updated_at = EXCLUDED.updated_at,
             revision = EXCLUDED.revision,
             deleted = EXCLUDED.deleted
           WHERE EXCLUDED.updated_at > sync_records.updated_at
              OR (EXCLUDED.updated_at = sync_records.updated_at AND EXCLUDED.client_id > sync_records.client_id)`,
          [m.entity, m.entityId, m.clientId, JSON.stringify(m.payload), m.updatedAt, m.revision, m.deleted],
        );
        appliedCount++;
      }
      await client.query('COMMIT');
      return { ackedKeys, appliedCount, duplicateCount };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async pull(afterSeq: number, limit = 100): Promise<PullResult> {
    const res = await this.pool.query<PgRecordRow>(
      `SELECT server_seq, entity, entity_id, client_id, payload, updated_at, revision, deleted
       FROM sync_records WHERE server_seq > $1 ORDER BY server_seq ASC LIMIT $2`,
      [afterSeq, limit],
    );
    const records = res.rows.map((r) => ({
      serverSeq: Number(r.server_seq),
      entity: r.entity,
      entityId: r.entity_id,
      clientId: r.client_id,
      payload: r.payload,
      updatedAt: r.updated_at.toISOString(),
      revision: r.revision,
      deleted: r.deleted,
    }));
    return { records, lastSeq: records[records.length - 1]?.serverSeq ?? afterSeq };
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
