import { z } from 'zod';

export const UtcTimestampSchema = z
  .string()
  .refine((val) => !Number.isNaN(Date.parse(val)), { message: 'Invalid ISO-8601 timestamp' })
  .transform((val) => new Date(val).toISOString());

export const JsonObjectSchema = z.record(z.string(), z.unknown());

export const MutationSchema = z.object({
  idempotencyKey: z.string().uuid(),
  entity: z.string().min(1).max(64).regex(/^[a-zA-Z0-9_:-]+$/),
  entityId: z.string().min(1).max(128),
  clientId: z.string().min(1).max(64),
  payload: JsonObjectSchema,
  updatedAt: UtcTimestampSchema,
  revision: z.number().int().positive(),
  deleted: z.boolean(),
});

export type Mutation = z.infer<typeof MutationSchema>;

export interface SyncedRecord {
  serverSeq: number;
  entity: string;
  entityId: string;
  clientId: string;
  payload: Record<string, unknown>;
  updatedAt: string;
  revision: number;
  deleted: boolean;
}

export interface PushResult {
  ackedKeys: string[];
  appliedCount: number;
  duplicateCount: number;
}

export interface PullResult {
  records: SyncedRecord[];
  lastSeq: number;
}

export interface RemoteStore {
  push(mutations: Mutation[]): Promise<PushResult>;
  pull(afterSeq: number, limit?: number): Promise<PullResult>;
}

export interface RetryOptions {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

export interface SyncResult {
  pushed: number;
  duplicatesSkipped: number;
  pulled: number;
  cursor: number;
}
