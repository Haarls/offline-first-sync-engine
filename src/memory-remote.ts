import { isIncomingNewer } from './lww.js';
import {
  MutationSchema,
  type Mutation,
  type PullResult,
  type PushResult,
  type RemoteStore,
  type SyncedRecord,
} from './types.js';

export class MemoryRemote implements RemoteStore {
  private seq = 0;
  private readonly appliedKeys = new Set<string>();
  private readonly records = new Map<string, SyncedRecord>();

  async push(mutations: Mutation[]): Promise<PushResult> {
    const ackedKeys: string[] = [];
    let appliedCount = 0;
    let duplicateCount = 0;

    for (const raw of mutations) {
      const m = MutationSchema.parse(raw);
      if (this.appliedKeys.has(m.idempotencyKey)) {
        duplicateCount++;
        ackedKeys.push(m.idempotencyKey);
        continue;
      }

      this.appliedKeys.add(m.idempotencyKey);
      const compositeKey = `${m.entity}:${m.entityId}`;
      const existing = this.records.get(compositeKey);

      if (
        !existing ||
        isIncomingNewer(
          { updatedAt: m.updatedAt, clientId: m.clientId },
          { updatedAt: existing.updatedAt, clientId: existing.clientId },
        )
      ) {
        this.seq += 1;
        this.records.set(compositeKey, {
          serverSeq: this.seq,
          entity: m.entity,
          entityId: m.entityId,
          clientId: m.clientId,
          payload: structuredClone(m.payload),
          updatedAt: m.updatedAt,
          revision: m.revision,
          deleted: m.deleted,
        });
      }

      appliedCount++;
      ackedKeys.push(m.idempotencyKey);
    }

    return { ackedKeys, appliedCount, duplicateCount };
  }

  async pull(afterSeq: number, limit = 100): Promise<PullResult> {
    const records = Array.from(this.records.values())
      .filter((r) => r.serverSeq > afterSeq)
      .sort((a, b) => a.serverSeq - b.serverSeq)
      .slice(0, limit)
      .map((r) => ({ ...r, payload: structuredClone(r.payload) }));

    const last = records[records.length - 1];
    return {
      records,
      lastSeq: last ? last.serverSeq : afterSeq,
    };
  }
}
