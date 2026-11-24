import type { RetryOptions } from './types.js';

export interface VersionStamp {
  updatedAt: string;
  clientId: string;
}

export function isIncomingNewer(incoming: VersionStamp, existing: VersionStamp): boolean {
  const incomingMs = Date.parse(incoming.updatedAt);
  const existingMs = Date.parse(existing.updatedAt);
  if (incomingMs !== existingMs) {
    return incomingMs > existingMs;
  }
  // Code-unit tie-breaker matches PostgreSQL COLLATE "C" so concurrent edits converge identically on every node.
  return incoming.clientId > existing.clientId;
}

export function computeBackoffDelay(
  attempt: number,
  baseDelayMs = 150,
  maxDelayMs = 5_000,
  random: () => number = Math.random,
): number {
  const ceiling = Math.min(maxDelayMs, baseDelayMs * 2 ** attempt);
  const half = ceiling / 2;
  return Math.round(half + random() * half);
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

export async function withBackoff<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {},
): Promise<T> {
  const maxAttempts = Math.max(1, options.maxAttempts ?? 4);
  const baseDelayMs = options.baseDelayMs ?? 150;
  const maxDelayMs = options.maxDelayMs ?? 5_000;
  const sleep = options.sleep ?? defaultSleep;
  const random = options.random ?? Math.random;

  let lastError: unknown;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (attempt + 1 >= maxAttempts) {
        break;
      }
      await sleep(computeBackoffDelay(attempt, baseDelayMs, maxDelayMs, random));
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}
