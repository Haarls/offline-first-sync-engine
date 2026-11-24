import { afterEach, describe, expect, it } from 'vitest';
import {
  buildPgPoolConfig,
  freezeCurrencyTx,
  LocalSyncNode,
  MemoryRemote,
  type Mutation,
  type PullResult,
  type PushResult,
  type RemoteStore,
} from '../src/index.js';

const openNodes: LocalSyncNode[] = [];

function createNode(clientId: string): LocalSyncNode {
  const node = new LocalSyncNode({ dbPath: ':memory:', clientId });
  openNodes.push(node);
  return node;
}

afterEach(() => {
  while (openNodes.length > 0) {
    openNodes.pop()?.close();
  }
});

describe('offline-first-sync-engine', () => {
  it('queues mutations in SQLite outbox while offline and syncs bidirectionally on reconnect', async () => {
    const remote = new MemoryRemote();
    const posA = createNode('pos-terminal-a');
    const posB = createNode('pos-terminal-b');

    const m1 = posA.put('product', 'sku-101', { name: 'Espresso Beans 1kg', stock: 24 });
    const m2 = posA.put('product', 'sku-101', { name: 'Espresso Beans 1kg', stock: 21 });

    expect(m1.revision).toBe(1);
    expect(m2.revision).toBe(2);
    expect(posA.pendingOutbox()).toHaveLength(2);

    const syncA = await posA.sync(remote);
    expect(syncA.pushed).toBe(2);
    expect(posA.pendingOutbox()).toHaveLength(0);

    const syncB = await posB.sync(remote);
    expect(syncB.pulled).toBe(1);
    expect(posB.get<{ name: string; stock: number }>('product', 'sku-101')).toEqual({
      name: 'Espresso Beans 1kg',
      stock: 21,
    });
  });

  it('resolves offline LWW conflicts deterministically on both timestamp order and exact millisecond ties', async () => {
    const remote = new MemoryRemote();
    const nodeA = createNode('branch-01');
    const nodeB = createNode('branch-02');

    // Case 1: Distinct timestamps — later timestamp wins regardless of push order
    nodeB.put('inventory', 'item-1', { qty: 50 }, { updatedAt: '2026-11-23T15:00:05.000Z' });
    nodeA.put('inventory', 'item-1', { qty: 12 }, { updatedAt: '2026-11-23T15:00:01.000Z' });

    await nodeB.sync(remote);
    await nodeA.sync(remote);
    await nodeB.sync(remote);

    expect(nodeA.get('inventory', 'item-1')).toEqual({ qty: 50 });
    expect(nodeB.get('inventory', 'item-1')).toEqual({ qty: 50 });

    // Case 2: Exact UTC millisecond tie — higher clientId ('branch-02' > 'branch-01') wins on all nodes
    const exactTie = '2026-11-23T16:00:00.000Z';
    nodeA.put('inventory', 'item-2', { manager: 'Alice' }, { updatedAt: exactTie });
    nodeB.put('inventory', 'item-2', { manager: 'Bruno' }, { updatedAt: exactTie });

    // Push higher clientId first, then lower clientId, and pull back on both
    await nodeB.sync(remote);
    await nodeA.sync(remote);
    await nodeB.sync(remote);

    expect(nodeA.get('inventory', 'item-2')).toEqual({ manager: 'Bruno' });
    expect(nodeB.get('inventory', 'item-2')).toEqual({ manager: 'Bruno' });
  });

  it('deduplicates replayed batches via idempotency_key when network drops after server commit', async () => {
    const inner = new MemoryRemote();
    let dropFirstPushResponse = true;

    const flakyRemote: RemoteStore = {
      async push(mutations: Mutation[]): Promise<PushResult> {
        const res = await inner.push(mutations);
        if (dropFirstPushResponse) {
          dropFirstPushResponse = false;
          throw new Error('ECONNRESET: socket hang up after remote commit');
        }
        return res;
      },
      pull(afterSeq: number, limit?: number): Promise<PullResult> {
        return inner.pull(afterSeq, limit);
      },
    };

    const node = createNode('pos-register-3');
    node.put('order', 'ord-900', { totalUsd: 42.5 });

    await expect(
      node.sync(flakyRemote, { retry: { maxAttempts: 1 } }),
    ).rejects.toThrow('ECONNRESET');

    // Outbox retains unacknowledged mutation because the ACK never arrived
    expect(node.pendingOutbox()).toHaveLength(1);

    const recovery = await node.sync(flakyRemote);
    expect(recovery.pushed).toBe(0);
    expect(recovery.duplicatesSkipped).toBe(1);
    expect(node.pendingOutbox()).toHaveLength(0);
  });

  it('applies exponential backoff with bounded jitter across transient offline failures', async () => {
    const inner = new MemoryRemote();
    let failuresRemaining = 3;

    const unstableRemote: RemoteStore = {
      async push(mutations: Mutation[]): Promise<PushResult> {
        if (failuresRemaining > 0) {
          failuresRemaining--;
          throw new Error('ENETUNREACH');
        }
        return inner.push(mutations);
      },
      pull(afterSeq: number, limit?: number): Promise<PullResult> {
        return inner.pull(afterSeq, limit);
      },
    };

    const delays: number[] = [];
    const node = createNode('pos-mobile-1');
    node.put('shift', 'shift-44', { status: 'OPEN' });

    const result = await node.sync(unstableRemote, {
      retry: {
        maxAttempts: 5,
        baseDelayMs: 100,
        maxDelayMs: 2_000,
        random: () => 0.5,
        sleep: async (ms) => {
          delays.push(ms);
        },
      },
    });

    expect(result.pushed).toBe(1);
    // With base=100 and random=0.5 (75% of ceiling): 75ms, 150ms, 300ms
    expect(delays).toEqual([75, 150, 300]);
  });

  it('preserves historical multi-currency snapshots across subsequent exchange rate shifts', async () => {
    const remote = new MemoryRemote();
    const cashier = createNode('pos-caja-1');
    const auditor = createNode('hq-audit');

    let liveVesRate = 36.5;
    const morningPayment = freezeCurrencyTx({
      amountUsd: 19.99,
      quoteCurrency: 'VES',
      exchangeRate: liveVesRate,
      capturedAt: '2026-11-23T12:00:00.000Z',
    });

    cashier.put('sale', 'inv-001', { payment: morningPayment });

    // Afternoon devaluation changes live rate; historical snapshot must remain untouched
    liveVesRate = 42.85;
    const afternoonPayment = freezeCurrencyTx({
      amountUsd: 19.99,
      quoteCurrency: 'VES',
      exchangeRate: liveVesRate,
      capturedAt: '2026-11-23T18:00:00.000Z',
    });

    cashier.put('sale', 'inv-002', { payment: afternoonPayment });

    await cashier.sync(remote);
    await auditor.sync(remote);

    const syncedMorning = auditor.get<{ payment: typeof morningPayment }>('sale', 'inv-001');
    const syncedAfternoon = auditor.get<{ payment: typeof afternoonPayment }>('sale', 'inv-002');

    expect(syncedMorning?.payment.exchangeRate).toBe(36.5);
    expect(syncedMorning?.payment.convertedAmount).toBe(729.64);
    expect(syncedAfternoon?.payment.exchangeRate).toBe(42.85);
    expect(syncedAfternoon?.payment.convertedAmount).toBe(856.57);
  });

  it('propagates tombstone deletions across offline nodes without resurrecting older state', async () => {
    const remote = new MemoryRemote();
    const nodeA = createNode('node-a');
    const nodeB = createNode('node-b');

    nodeA.put('draft', 'd-1', { note: 'temp' }, { updatedAt: '2026-11-23T10:00:00.000Z' });
    await nodeA.sync(remote);
    await nodeB.sync(remote);
    expect(nodeB.get('draft', 'd-1')).toEqual({ note: 'temp' });

    nodeA.remove('draft', 'd-1', { updatedAt: '2026-11-23T10:05:00.000Z' });
    await nodeA.sync(remote);
    await nodeB.sync(remote);

    expect(nodeA.get('draft', 'd-1')).toBeNull();
    expect(nodeB.get('draft', 'd-1')).toBeNull();
  });

  it('configures Neon SSL cleanly when sslmode=require or neon.tech host is provided', () => {
    const neonCfg = buildPgPoolConfig(
      'postgresql://haarls:secret@ep-cool-frog-123.us-east-2.aws.neon.tech/neondb?sslmode=require',
    );
    expect(neonCfg.ssl).toEqual({ rejectUnauthorized: true });
    expect(neonCfg.connectionString).not.toContain('sslmode=');

    const localCfg = buildPgPoolConfig('postgresql://postgres:postgres@localhost:5432/sync_engine');
    expect(localCfg.ssl).toBeUndefined();
  });
});
