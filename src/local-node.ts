import { randomUUID } from 'node:crypto';
import Database from 'better-sqlite3';
import { isIncomingNewer } from './lww.js';
import { withBackoff } from './lww.js';
import {
  MutationSchema,
  type Mutation,
  type RemoteStore,
  type RetryOptions,
  type SyncResult,
  type SyncedRecord,
} from './types.js';

interface RecordRow {
  entity: string;
  entity_id: string;
  client_id: string;
  payload: string;
  updated_at: string;
  revision: number;
  deleted: number;
}

interface OutboxRow extends RecordRow {
  seq: number;
  idempotency_key: string;
}

export interface LocalNodeOptions {
  dbPath: string;
  clientId: string;
}

export interface WriteOptions {
  updatedAt?: string;
  idempotencyKey?: string;
}

export interface SyncOptions {
  batchSize?: number;
  retry?: RetryOptions;
}

export class LocalSyncNode {
  readonly clientId: string;
  private readonly db: Database.Database;

  constructor(options: LocalNodeOptions) {
    if (!options.clientId.trim()) {
      throw new Error('clientId must be a non-empty string');
    }
    this.clientId = options.clientId;
    this.db = new Database(options.dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.initSchema();
  }

  private initSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS records (
        entity TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        client_id TEXT NOT NULL,
        payload TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        revision INTEGER NOT NULL,
        deleted INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (entity, entity_id)
      );

      CREATE TABLE IF NOT EXISTS outbox (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        idempotency_key TEXT NOT NULL UNIQUE,
        entity TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        client_id TEXT NOT NULL,
        payload TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        revision INTEGER NOT NULL,
        deleted INTEGER NOT NULL DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS sync_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    `);
  }

  put<T extends Record<string, unknown>>(
    entity: string,
    entityId: string,
    payload: T,
    options?: WriteOptions,
  ): Mutation {
    return this.writeMutation(entity, entityId, payload, false, options);
  }

  remove(entity: string, entityId: string, options?: WriteOptions): Mutation {
    return this.writeMutation(entity, entityId, {}, true, options);
  }

  private writeMutation(
    entity: string,
    entityId: string,
    payload: Record<string, unknown>,
    deleted: boolean,
    options?: WriteOptions,
  ): Mutation {
    const run = this.db.transaction((): Mutation => {
      const existing = this.db
        .prepare<[string, string], RecordRow>(
          'SELECT entity, entity_id, client_id, payload, updated_at, revision, deleted FROM records WHERE entity = ? AND entity_id = ?',
        )
        .get(entity, entityId);

      // Monotonic guard prevents sub-millisecond writes on the same node from colliding on equal (updatedAt, clientId).
      const nextTimestamp =
        options?.updatedAt ??
        new Date(
          Math.max(Date.now(), existing ? Date.parse(existing.updated_at) + 1 : 0),
        ).toISOString();

      const mutation = MutationSchema.parse({
        idempotencyKey: options?.idempotencyKey ?? randomUUID(),
        entity,
        entityId,
        clientId: this.clientId,
        payload,
        updatedAt: nextTimestamp,
        revision: (existing?.revision ?? 0) + 1,
        deleted,
      });

      const serialized = JSON.stringify(mutation.payload);

      if (
        !existing ||
        isIncomingNewer(
          { updatedAt: mutation.updatedAt, clientId: mutation.clientId },
          { updatedAt: existing.updated_at, clientId: existing.client_id },
        )
      ) {
        this.db
          .prepare(
            `INSERT INTO records (entity, entity_id, client_id, payload, updated_at, revision, deleted)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(entity, entity_id) DO UPDATE SET
               client_id = excluded.client_id,
               payload = excluded.payload,
               updated_at = excluded.updated_at,
               revision = excluded.revision,
               deleted = excluded.deleted`,
          )
          .run(
            mutation.entity,
            mutation.entityId,
            mutation.clientId,
            serialized,
            mutation.updatedAt,
            mutation.revision,
            mutation.deleted ? 1 : 0,
          );
      }

      this.db
        .prepare(
          `INSERT INTO outbox (idempotency_key, entity, entity_id, client_id, payload, updated_at, revision, deleted)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          mutation.idempotencyKey,
          mutation.entity,
          mutation.entityId,
          mutation.clientId,
          serialized,
          mutation.updatedAt,
          mutation.revision,
          mutation.deleted ? 1 : 0,
        );

      return mutation;
    });

    return run();
  }

  get<T = Record<string, unknown>>(entity: string, entityId: string): T | null {
    const row = this.db
      .prepare<[string, string], Pick<RecordRow, 'payload' | 'deleted'>>(
        'SELECT payload, deleted FROM records WHERE entity = ? AND entity_id = ?',
      )
      .get(entity, entityId);

    if (!row || row.deleted === 1) {
      return null;
    }
    return JSON.parse(row.payload) as T;
  }

  list<T = Record<string, unknown>>(entity: string): Array<{ id: string; data: T; revision: number; updatedAt: string }> {
    const rows = this.db
      .prepare<[string], RecordRow>(
        'SELECT entity, entity_id, client_id, payload, updated_at, revision, deleted FROM records WHERE entity = ? AND deleted = 0 ORDER BY entity_id ASC',
      )
      .all(entity);

    return rows.map((row) => ({
      id: row.entity_id,
      data: JSON.parse(row.payload) as T,
      revision: row.revision,
      updatedAt: row.updated_at,
    }));
  }

  pendingOutbox(): Mutation[] {
    const rows = this.db
      .prepare<[], OutboxRow>(
        'SELECT seq, idempotency_key, entity, entity_id, client_id, payload, updated_at, revision, deleted FROM outbox ORDER BY seq ASC',
      )
      .all();

    return rows.map((row) => ({
      idempotencyKey: row.idempotency_key,
      entity: row.entity,
      entityId: row.entity_id,
      clientId: row.client_id,
      payload: JSON.parse(row.payload) as Record<string, unknown>,
      updatedAt: row.updated_at,
      revision: row.revision,
      deleted: row.deleted === 1,
    }));
  }

  async sync(remote: RemoteStore, options: SyncOptions = {}): Promise<SyncResult> {
    const batchSize = Math.max(1, options.batchSize ?? 100);
    let pushed = 0;
    let duplicatesSkipped = 0;

    while (true) {
      const batch = this.pendingOutbox().slice(0, batchSize);
      if (batch.length === 0) {
        break;
      }

      const pushRes = await withBackoff(
        () => remote.push(batch),
        options.retry,
      );

      if (pushRes.ackedKeys.length === 0) {
        throw new Error('Remote acknowledged 0 mutations for non-empty batch');
      }

      const deleteStmt = this.db.prepare('DELETE FROM outbox WHERE idempotency_key = ?');
      const ackTx = this.db.transaction((keys: string[]) => {
        for (const key of keys) {
          deleteStmt.run(key);
        }
      });
      ackTx(pushRes.ackedKeys);

      pushed += pushRes.appliedCount;
      duplicatesSkipped += pushRes.duplicateCount;
    }

    let cursor = this.getCursor();
    let pulled = 0;

    const applyPullPage = this.db.transaction((records: SyncedRecord[], nextSeq: number): number => {
      let applied = 0;
      const selectExisting = this.db.prepare<[string, string], Pick<RecordRow, 'updated_at' | 'client_id'>>(
        'SELECT updated_at, client_id FROM records WHERE entity = ? AND entity_id = ?',
      );
      const upsertRecord = this.db.prepare(
        `INSERT INTO records (entity, entity_id, client_id, payload, updated_at, revision, deleted)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(entity, entity_id) DO UPDATE SET
           client_id = excluded.client_id,
           payload = excluded.payload,
           updated_at = excluded.updated_at,
           revision = excluded.revision,
           deleted = excluded.deleted`,
      );

      for (const rec of records) {
        const existing = selectExisting.get(rec.entity, rec.entityId);
        if (
          !existing ||
          isIncomingNewer(
            { updatedAt: rec.updatedAt, clientId: rec.clientId },
            { updatedAt: existing.updated_at, clientId: existing.client_id },
          )
        ) {
          upsertRecord.run(
            rec.entity,
            rec.entityId,
            rec.clientId,
            JSON.stringify(rec.payload),
            rec.updatedAt,
            rec.revision,
            rec.deleted ? 1 : 0,
          );
          applied++;
        }
      }

      this.db
        .prepare(
          `INSERT INTO sync_meta (key, value) VALUES ('last_seq', ?)
           ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
        )
        .run(String(nextSeq));

      return applied;
    });

    while (true) {
      const pullRes = await withBackoff(
        () => remote.pull(cursor, batchSize),
        options.retry,
      );

      if (pullRes.records.length > 0 || pullRes.lastSeq > cursor) {
        pulled += applyPullPage(pullRes.records, pullRes.lastSeq);
        cursor = pullRes.lastSeq;
      }

      if (pullRes.records.length < batchSize) {
        break;
      }
    }

    return { pushed, duplicatesSkipped, pulled, cursor };
  }

  private getCursor(): number {
    const row = this.db
      .prepare<[], { value: string }>("SELECT value FROM sync_meta WHERE key = 'last_seq'")
      .get();
    return row ? Number(row.value) : 0;
  }

  close(): void {
    this.db.close();
  }
}
