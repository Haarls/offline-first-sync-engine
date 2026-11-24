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

export function buildPgPoolConfig(connectionString: string, max = 5): PoolConfig {
  const parsed = new URL(connectionString);
  const sslMode = parsed.searchParams.get('sslmode');
  const isNeonHost = parsed.hostname.endsWith('.neon.tech');
  const useTls =
    isNeonHost ||
    sslMode === 'require' ||
    sslMode === 'verify-ca' ||
    sslMode === 'verify-full';

  // Strip sslmode from URL when enabling explicit TLS options so pg-connection-string does not override rejectUnauthorized.
  if (useTls && sslMode) {
    parsed.searchParams.delete('sslmode');
  }

  const config: PoolConfig = {
    connectionString: parsed.toString(),
    max,
    idleTimeoutMillis: 10_000,
    connectionTimeoutMillis: 5_000,
  };

  if (useTls) {
    config.ssl = { rejectUnauthorized: true };
  }

  return config;
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
        const dedup = await client.query<{ idempotency_key: string }>(
          `INSERT INTO sync_applied_mutations (idempotency_key)
           VALUES ($1)
           ON CONFLICT (idempotency_key) DO NOTHING
           RETURNING idempotency_key`,
          [m.idempotencyKey],
        );

        if (dedup.rowCount === 0) {
          duplicateCount++;
          ackedKeys.push(m.idempotencyKey);
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
          [
            m.entity,
            m.entityId,
            m.clientId,
            JSON.stringify(m.payload),
            m.updatedAt,
            m.revision,
            m.deleted,
          ],
        );

        appliedCount++;
        ackedKeys.push(m.idempotencyKey);
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
       FROM sync_records
       WHERE server_seq > $1
       ORDER BY server_seq ASC
       LIMIT $2`,
      [afterSeq, limit],
    );

    const records = res.rows.map((row) => ({
      serverSeq: Number(row.server_seq),
      entity: row.entity,
      entityId: row.entity_id,
      clientId: row.client_id,
      payload: row.payload,
      updatedAt: row.updated_at.toISOString(),
      revision: row.revision,
      deleted: row.deleted,
    }));

    const lastRecord = records[records.length - 1];
    return {
      records,
      lastSeq: lastRecord ? lastRecord.serverSeq : afterSeq,
    };
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
