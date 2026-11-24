export interface RecordRow {
  entity: string;
  entity_id: string;
  client_id: string;
  payload: string;
  updated_at: string;
  revision: number;
  deleted: number;
}

export const SCHEMA_SQL = `
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
`;

export const UPSERT_RECORD_SQL = `
  INSERT INTO records (entity, entity_id, client_id, payload, updated_at, revision, deleted)
  VALUES (?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(entity, entity_id) DO UPDATE SET
    client_id = excluded.client_id,
    payload = excluded.payload,
    updated_at = excluded.updated_at,
    revision = excluded.revision,
    deleted = excluded.deleted
`;

export const INSERT_OUTBOX_SQL = `
  INSERT INTO outbox (idempotency_key, entity, entity_id, client_id, payload, updated_at, revision, deleted)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`;
