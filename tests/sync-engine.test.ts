import { afterEach, describe, expect, it } from 'vitest';
import {
  buildPgPoolConfig,
  freezeCurrencyTx,
  LocalSyncNode,
  MemoryRemote,
  type Mutation,
  type RemoteStore,
} from '../src/index.js';

const openNodes: LocalSyncNode[] = [];
function createNode(clientId: string): LocalSyncNode {
  const node = new LocalSyncNode({ dbPath: ':memory:', clientId });
  openNodes.push(node);
  return node;
}

afterEach(() => {
  while (openNodes.length > 0) openNodes.pop()?.close();
});

describe('offline-first-sync-engine', () => {
  it('queues mutations in SQLite outbox while offline and syncs bidirectionally on reconnect', async () => {
    const remote = new MemoryRemote();
    const [posA, posB] = [createNode('pos-terminal-a'), createNode('pos-terminal-b')];

    expect(posA.put('product', 'sku-101', { name: 'Espresso Beans 1kg', stock: 24 }).revision).toBe(1);
    expect(posA.put('product', 'sku-101', { name: 'Espresso Beans 1kg', stock: 21 }).revision).toBe(2);
    expect(posA.pendingOutbox()).toHaveLength(2);

    expect((await posA.sync(remote)).pushed).toBe(2);
    expect(posA.pendingOutbox()).toHaveLength(0);

    expect((await posB.sync(remote)).pulled).toBe(1);
    expect(posB.get('product', 'sku-101')).toEqual({ name: 'Espresso Beans 1kg', stock: 21 });
  });

  it('resolves offline LWW conflicts deterministically on both timestamp order and exact millisecond ties', async () => {
    const remote = new MemoryRemote();
    const [nodeA, nodeB] = [createNode('branch-01'), createNode('branch-02')];

    nodeB.put('inventory', 'item-1', { qty: 50 }, { updatedAt: '2026-11-23T15:00:05.000Z' });
    nodeA.put('inventory', 'item-1', { qty: 12 }, { updatedAt: '2026-11-23T15:00:01.000Z' });
    await nodeB.sync(remote);
    await nodeA.sync(remote);
    await nodeB.sync(remote);
    expect(nodeA.get('inventory', 'item-1')).toEqual({ qty: 50 });
    expect(nodeB.get('inventory', 'item-1')).toEqual({ qty: 50 });

    // Exact UTC tie: higher clientId ('branch-02' > 'branch-01') wins on every node
    const exactTie = '2026-11-23T16:00:00.000Z';
    nodeA.put('inventory', 'item-2', { manager: 'Alice' }, { updatedAt: exactTie });
    nodeB.put('inventory', 'item-2', { manager: 'Bruno' }, { updatedAt: exactTie });
    await nodeB.sync(remote);
    await nodeA.sync(remote);
    await nodeB.sync(remote);
    expect(nodeA.get('inventory', 'item-2')).toEqual({ manager: 'Bruno' });
    expect(nodeB.get('inventory', 'item-2')).toEqual({ manager: 'Bruno' });
  });

  it('deduplicates replayed batches via idempotency_key when network drops after server commit', async () => {
    const inner = new MemoryRemote();
    let dropFirstAck = true;
    const flakyRemote: RemoteStore = {
      async push(mutations: Mutation[]) {
        const res = await inner.push(mutations);
        if (dropFirstAck) {
          dropFirstAck = false;
          throw new Error('ECONNRESET: socket hang up after remote commit');
        }
        return res;
      },
      pull: (afterSeq, limit) => inner.pull(afterSeq, limit),
    };

    const node = createNode('pos-register-3');
    node.put('order', 'ord-900', { totalUsd: 42.5 });
    await expect(node.sync(flakyRemote, { retry: { maxAttempts: 1 } })).rejects.toThrow('ECONNRESET');
    expect(node.pendingOutbox()).toHaveLength(1);

    const recovery = await node.sync(flakyRemote);
    expect(recovery).toMatchObject({ pushed: 0, duplicatesSkipped: 1 });
    expect(node.pendingOutbox()).toHaveLength(0);
  });

  it('applies exponential backoff with bounded jitter across transient offline failures', async () => {
    const inner = new MemoryRemote();
    let failures = 3;
    const unstable: RemoteStore = {
      push: async (m) => (failures-- > 0 ? Promise.reject(new Error('ENETUNREACH')) : inner.push(m)),
      pull: (afterSeq, limit) => inner.pull(afterSeq, limit),
    };

    const delays: number[] = [];
    const node = createNode('pos-mobile-1');
    node.put('shift', 'shift-44', { status: 'OPEN' });

    const res = await node.sync(unstable, {
      retry: { maxAttempts: 5, baseDelayMs: 100, maxDelayMs: 2_000, random: () => 0.5, sleep: async (ms) => void delays.push(ms) },
    });
    expect(res.pushed).toBe(1);
    expect(delays).toEqual([75, 150, 300]);
  });

  it('preserves historical multi-currency snapshots across subsequent exchange rate shifts', async () => {
    const remote = new MemoryRemote();
    const [cashier, auditor] = [createNode('pos-caja-1'), createNode('hq-audit')];

    const morning = freezeCurrencyTx({ amountUsd: 19.99, quoteCurrency: 'VES', exchangeRate: 36.5, capturedAt: '2026-11-23T12:00:00.000Z' });
    cashier.put('sale', 'inv-001', { payment: morning });

    const afternoon = freezeCurrencyTx({ amountUsd: 19.99, quoteCurrency: 'VES', exchangeRate: 42.85, capturedAt: '2026-11-23T18:00:00.000Z' });
    cashier.put('sale', 'inv-002', { payment: afternoon });

    await cashier.sync(remote);
    await auditor.sync(remote);

    expect(auditor.get<{ payment: typeof morning }>('sale', 'inv-001')?.payment).toMatchObject({ exchangeRate: 36.5, convertedAmount: 729.64 });
    expect(auditor.get<{ payment: typeof afternoon }>('sale', 'inv-002')?.payment).toMatchObject({ exchangeRate: 42.85, convertedAmount: 856.57 });
  });

  it('propagates tombstone deletions across offline nodes without resurrecting older state', async () => {
    const remote = new MemoryRemote();
    const [nodeA, nodeB] = [createNode('node-a'), createNode('node-b')];

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
    const neonCfg = buildPgPoolConfig('postgresql://haarls:secret@ep-cool-frog-123.us-east-2.aws.neon.tech/neondb?sslmode=require');
    expect(neonCfg.ssl).toEqual({ rejectUnauthorized: true });
    expect(neonCfg.connectionString).not.toContain('sslmode=');

    expect(buildPgPoolConfig('postgresql://postgres:postgres@localhost:5432/sync_engine').ssl).toBeUndefined();
  });
});
