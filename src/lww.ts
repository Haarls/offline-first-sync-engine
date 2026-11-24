import type { RetryOptions } from './types.js';

export interface VersionStamp {
  updatedAt: string;
  clientId: string;
}

export function isIncomingNewer(incoming: VersionStamp, existing: VersionStamp): boolean {
  const delta = Date.parse(incoming.updatedAt) - Date.parse(existing.updatedAt);
  // Code-unit tie-breaker matches PostgreSQL COLLATE "C" so concurrent edits converge identically on every node.
  return delta !== 0 ? delta > 0 : incoming.clientId > existing.clientId;
}

export function computeBackoffDelay(
  attempt: number,
  baseDelayMs = 150,
  maxDelayMs = 5_000,
  random: () => number = Math.random,
): number {
  const ceiling = Math.min(maxDelayMs, baseDelayMs * 2 ** attempt);
  return Math.round(ceiling * (0.5 + random() * 0.5));
}

export async function withBackoff<T>(
  operation: () => Promise<T>,
  {
    maxAttempts = 4,
    baseDelayMs = 150,
    maxDelayMs = 5_000,
    sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    random = Math.random,
  }: RetryOptions = {},
): Promise<T> {
  const limit = Math.max(1, maxAttempts);
  let lastError: unknown;
  for (let attempt = 0; attempt < limit; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (attempt + 1 < limit) {
        await sleep(computeBackoffDelay(attempt, baseDelayMs, maxDelayMs, random));
      }
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}
