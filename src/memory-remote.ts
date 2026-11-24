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
      ackedKeys.push(m.idempotencyKey);
      if (this.appliedKeys.has(m.idempotencyKey)) {
        duplicateCount++;
        continue;
      }

      this.appliedKeys.add(m.idempotencyKey);
      const key = `${m.entity}:${m.entityId}`;
      const existing = this.records.get(key);

      if (!existing || isIncomingNewer(m, existing)) {
        this.records.set(key, {
          entity: m.entity,
          entityId: m.entityId,
          clientId: m.clientId,
          payload: structuredClone(m.payload),
          updatedAt: m.updatedAt,
          revision: m.revision,
          deleted: m.deleted,
          serverSeq: ++this.seq,
        });
      }
      appliedCount++;
    }

    return { ackedKeys, appliedCount, duplicateCount };
  }

  async pull(afterSeq: number, limit = 100): Promise<PullResult> {
    const records = Array.from(this.records.values())
      .filter((r) => r.serverSeq > afterSeq)
      .sort((a, b) => a.serverSeq - b.serverSeq)
      .slice(0, limit)
      .map((r) => ({ ...r, payload: structuredClone(r.payload) }));

    return { records, lastSeq: records[records.length - 1]?.serverSeq ?? afterSeq };
  }
}
