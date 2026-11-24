import { randomUUID } from 'node:crypto';
import Database from 'better-sqlite3';
import { isIncomingNewer, withBackoff } from './lww.js';
import { INSERT_OUTBOX_SQL, SCHEMA_SQL, UPSERT_RECORD_SQL, type RecordRow } from './sqlite-schema.js';
import {
  MutationSchema,
  type Mutation,
  type RemoteStore,
  type RetryOptions,
  type SyncResult,
  type SyncedRecord,
} from './types.js';

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

  constructor({ dbPath, clientId }: LocalNodeOptions) {
    if (!clientId.trim()) throw new Error('clientId must be a non-empty string');
    this.clientId = clientId;
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.exec(SCHEMA_SQL);
  }

  put<T extends Record<string, unknown>>(entity: string, entityId: string, payload: T, opts?: WriteOptions): Mutation {
    return this.writeMutation(entity, entityId, payload, false, opts);
  }

  remove(entity: string, entityId: string, opts?: WriteOptions): Mutation {
    return this.writeMutation(entity, entityId, {}, true, opts);
  }

  private applyIfNewer(rec: Omit<Mutation, 'idempotencyKey'>): boolean {
    const cur = this.db
      .prepare<[string, string], Pick<RecordRow, 'updated_at' | 'client_id'>>(
        'SELECT updated_at, client_id FROM records WHERE entity = ? AND entity_id = ?',
      )
      .get(rec.entity, rec.entityId);

    if (cur && !isIncomingNewer(rec, { updatedAt: cur.updated_at, clientId: cur.client_id })) {
      return false;
    }
    this.db
      .prepare(UPSERT_RECORD_SQL)
      .run(rec.entity, rec.entityId, rec.clientId, JSON.stringify(rec.payload), rec.updatedAt, rec.revision, Number(rec.deleted));
    return true;
  }

  private writeMutation(
    entity: string,
    entityId: string,
    payload: Record<string, unknown>,
    deleted: boolean,
    opts?: WriteOptions,
  ): Mutation {
    return this.db.transaction((): Mutation => {
      const cur = this.db
        .prepare<[string, string], Pick<RecordRow, 'updated_at' | 'revision'>>(
          'SELECT updated_at, revision FROM records WHERE entity = ? AND entity_id = ?',
        )
        .get(entity, entityId);

      const minNextMs = cur ? Date.parse(cur.updated_at) + 1 : 0;
      const updatedAt = opts?.updatedAt ?? new Date(Math.max(Date.now(), minNextMs)).toISOString();
      const m = MutationSchema.parse({
        idempotencyKey: opts?.idempotencyKey ?? randomUUID(),
        entity,
        entityId,
        clientId: this.clientId,
        payload,
        updatedAt,
        revision: (cur?.revision ?? 0) + 1,
        deleted,
      });

      this.applyIfNewer(m);
      this.db
        .prepare(INSERT_OUTBOX_SQL)
        .run(m.idempotencyKey, m.entity, m.entityId, m.clientId, JSON.stringify(m.payload), m.updatedAt, m.revision, Number(m.deleted));
      return m;
    })();
  }

  get<T = Record<string, unknown>>(entity: string, entityId: string): T | null {
    const row = this.db
      .prepare<[string, string], Pick<RecordRow, 'payload' | 'deleted'>>(
        'SELECT payload, deleted FROM records WHERE entity = ? AND entity_id = ?',
      )
      .get(entity, entityId);
    return !row || row.deleted === 1 ? null : (JSON.parse(row.payload) as T);
  }

  list<T = Record<string, unknown>>(entity: string) {
    return this.db
      .prepare<[string], RecordRow>('SELECT * FROM records WHERE entity = ? AND deleted = 0 ORDER BY entity_id ASC')
      .all(entity)
      .map((r) => ({ id: r.entity_id, data: JSON.parse(r.payload) as T, revision: r.revision, updatedAt: r.updated_at }));
  }

  pendingOutbox(limit = 10_000): Mutation[] {
    return this.db
      .prepare<[number], RecordRow & { idempotency_key: string }>('SELECT * FROM outbox ORDER BY seq ASC LIMIT ?')
      .all(limit)
      .map((r) => ({
        idempotencyKey: r.idempotency_key,
        entity: r.entity,
        entityId: r.entity_id,
        clientId: r.client_id,
        payload: JSON.parse(r.payload) as Record<string, unknown>,
        updatedAt: r.updated_at,
        revision: r.revision,
        deleted: r.deleted === 1,
      }));
  }

  async sync(remote: RemoteStore, { batchSize = 100, retry }: SyncOptions = {}): Promise<SyncResult> {
    const pageSize = Math.max(1, batchSize);
    let pushed = 0;
    let duplicatesSkipped = 0;

    const ackOutbox = this.db.transaction((keys: string[]) => {
      const del = this.db.prepare('DELETE FROM outbox WHERE idempotency_key = ?');
      for (const key of keys) del.run(key);
    });

    for (let batch = this.pendingOutbox(pageSize); batch.length > 0; batch = this.pendingOutbox(pageSize)) {
      const res = await withBackoff(() => remote.push(batch), retry);
      if (res.ackedKeys.length === 0) throw new Error('Remote acknowledged 0 mutations for non-empty batch');
      ackOutbox(res.ackedKeys);
      pushed += res.appliedCount;
      duplicatesSkipped += res.duplicateCount;
    }

    const cursorRow = this.db.prepare<[], { value: string }>("SELECT value FROM sync_meta WHERE key = 'last_seq'").get();
    let cursor = cursorRow ? Number(cursorRow.value) : 0;
    let pulled = 0;

    const applyPage = this.db.transaction((records: SyncedRecord[], nextSeq: number): number => {
      const count = records.filter((rec) => this.applyIfNewer(rec)).length;
      this.db
        .prepare("INSERT INTO sync_meta (key, value) VALUES ('last_seq', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
        .run(String(nextSeq));
      return count;
    });

    while (true) {
      const res = await withBackoff(() => remote.pull(cursor, pageSize), retry);
      if (res.records.length > 0 || res.lastSeq > cursor) {
        pulled += applyPage(res.records, res.lastSeq);
        cursor = res.lastSeq;
      }
      if (res.records.length < pageSize) break;
    }

    return { pushed, duplicatesSkipped, pulled, cursor };
  }

  close(): void {
    this.db.close();
  }
}
