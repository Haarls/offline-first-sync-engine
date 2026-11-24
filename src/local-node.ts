import { randomUUID } from 'node:crypto';
import Database from 'better-sqlite3';
import { isIncomingNewer, withBackoff } from './lww.js';
import {
  MutationSchema,
  type Mutation,
  type RemoteStore,
  type RetryOptions,
  type SyncResult,
  type SyncedRecord,
} from './types.js';

interface RecordRow {
  entity: string; entity_id: string; client_id: string; payload: string;
  updated_at: string; revision: number; deleted: number;
}

export interface LocalNodeOptions { dbPath: string; clientId: string }
export interface WriteOptions { updatedAt?: string; idempotencyKey?: string }
export interface SyncOptions { batchSize?: number; retry?: RetryOptions }

const UPSERT_RECORD_SQL = `
  INSERT INTO records (entity, entity_id, client_id, payload, updated_at, revision, deleted)
  VALUES (?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(entity, entity_id) DO UPDATE SET
    client_id = excluded.client_id, payload = excluded.payload, updated_at = excluded.updated_at,
    revision = excluded.revision, deleted = excluded.deleted`;

const SCHEMA_SQL = `
  CREATE TABLE IF NOT EXISTS records (
    entity TEXT NOT NULL, entity_id TEXT NOT NULL, client_id TEXT NOT NULL, payload TEXT NOT NULL,
    updated_at TEXT NOT NULL, revision INTEGER NOT NULL, deleted INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (entity, entity_id)
  );
  CREATE TABLE IF NOT EXISTS outbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, idempotency_key TEXT NOT NULL UNIQUE, entity TEXT NOT NULL,
    entity_id TEXT NOT NULL, client_id TEXT NOT NULL, payload TEXT NOT NULL, updated_at TEXT NOT NULL,
    revision INTEGER NOT NULL, deleted INTEGER NOT NULL DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS sync_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);`;

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

  put<T extends Record<string, unknown>>(entity: string, entityId: string, payload: T, options?: WriteOptions): Mutation {
    return this.writeMutation(entity, entityId, payload, false, options);
  }

  remove(entity: string, entityId: string, options?: WriteOptions): Mutation {
    return this.writeMutation(entity, entityId, {}, true, options);
  }

  private applyIfNewer(rec: Omit<Mutation, 'idempotencyKey'>): boolean {
    const existing = this.db
      .prepare<[string, string], Pick<RecordRow, 'updated_at' | 'client_id'>>('SELECT updated_at, client_id FROM records WHERE entity = ? AND entity_id = ?')
      .get(rec.entity, rec.entityId);
    if (existing && !isIncomingNewer(rec, { updatedAt: existing.updated_at, clientId: existing.client_id })) return false;

    this.db.prepare(UPSERT_RECORD_SQL).run(rec.entity, rec.entityId, rec.clientId, JSON.stringify(rec.payload), rec.updatedAt, rec.revision, rec.deleted ? 1 : 0);
    return true;
  }

  private writeMutation(entity: string, entityId: string, payload: Record<string, unknown>, deleted: boolean, options?: WriteOptions): Mutation {
    return this.db.transaction((): Mutation => {
      const existing = this.db
        .prepare<[string, string], Pick<RecordRow, 'updated_at' | 'revision'>>('SELECT updated_at, revision FROM records WHERE entity = ? AND entity_id = ?')
        .get(entity, entityId);

      // Monotonic guard prevents sub-millisecond writes on the same node from colliding on equal (updatedAt, clientId).
      const updatedAt = options?.updatedAt ?? new Date(Math.max(Date.now(), existing ? Date.parse(existing.updated_at) + 1 : 0)).toISOString();
      const mutation = MutationSchema.parse({
        idempotencyKey: options?.idempotencyKey ?? randomUUID(),
        entity, entityId, clientId: this.clientId, payload, updatedAt,
        revision: (existing?.revision ?? 0) + 1, deleted,
      });

      this.applyIfNewer(mutation);
      this.db
        .prepare('INSERT INTO outbox (idempotency_key, entity, entity_id, client_id, payload, updated_at, revision, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
        .run(mutation.idempotencyKey, mutation.entity, mutation.entityId, mutation.clientId, JSON.stringify(mutation.payload), mutation.updatedAt, mutation.revision, mutation.deleted ? 1 : 0);
      return mutation;
    })();
  }

  get<T = Record<string, unknown>>(entity: string, entityId: string): T | null {
    const row = this.db
      .prepare<[string, string], Pick<RecordRow, 'payload' | 'deleted'>>('SELECT payload, deleted FROM records WHERE entity = ? AND entity_id = ?')
      .get(entity, entityId);
    return !row || row.deleted === 1 ? null : (JSON.parse(row.payload) as T);
  }

  list<T = Record<string, unknown>>(entity: string): Array<{ id: string; data: T; revision: number; updatedAt: string }> {
    return this.db
      .prepare<[string], RecordRow>('SELECT entity, entity_id, client_id, payload, updated_at, revision, deleted FROM records WHERE entity = ? AND deleted = 0 ORDER BY entity_id ASC')
      .all(entity)
      .map((r) => ({ id: r.entity_id, data: JSON.parse(r.payload) as T, revision: r.revision, updatedAt: r.updated_at }));
  }

  pendingOutbox(limit = 10_000): Mutation[] {
    return this.db
      .prepare<[number], RecordRow & { idempotency_key: string }>('SELECT idempotency_key, entity, entity_id, client_id, payload, updated_at, revision, deleted FROM outbox ORDER BY seq ASC LIMIT ?')
      .all(limit)
      .map((r) => ({
        idempotencyKey: r.idempotency_key, entity: r.entity, entityId: r.entity_id, clientId: r.client_id,
        payload: JSON.parse(r.payload) as Record<string, unknown>, updatedAt: r.updated_at, revision: r.revision, deleted: r.deleted === 1,
      }));
  }

  async sync(remote: RemoteStore, { batchSize = 100, retry }: SyncOptions = {}): Promise<SyncResult> {
    const pageSize = Math.max(1, batchSize);
    let [pushed, duplicatesSkipped] = [0, 0];

    const ackOutbox = this.db.transaction((keys: string[]) => {
      const del = this.db.prepare('DELETE FROM outbox WHERE idempotency_key = ?');
      for (const key of keys) del.run(key);
    });

    while (true) {
      const batch = this.pendingOutbox(pageSize);
      if (batch.length === 0) break;
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
      let count = 0;
      for (const rec of records) if (this.applyIfNewer(rec)) count++;
      this.db.prepare("INSERT INTO sync_meta (key, value) VALUES ('last_seq', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value").run(String(nextSeq));
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

  close(): void { this.db.close(); }
}
