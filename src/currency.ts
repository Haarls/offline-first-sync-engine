import { z } from 'zod';
import { UtcTimestampSchema } from './types.js';

const CurrencyInputSchema = z.object({
  amountUsd: z.number().finite().nonnegative(),
  quoteCurrency: z.string().regex(/^[A-Z]{3}$/, 'Expected ISO-4217 uppercase currency code'),
  exchangeRate: z.number().finite().positive(),
  capturedAt: UtcTimestampSchema.optional(),
});

export interface MonetarySnapshot {
  readonly baseCurrency: 'USD';
  readonly baseAmount: number;
  readonly quoteCurrency: string;
  readonly exchangeRate: number;
  readonly convertedAmount: number;
  readonly capturedAt: string;
}

export function freezeCurrencyTx(input: {
  amountUsd: number;
  quoteCurrency: string;
  exchangeRate: number;
  capturedAt?: string;
}): MonetarySnapshot {
  const parsed = CurrencyInputSchema.parse(input);
  const baseCents = Math.round(parsed.amountUsd * 100);
  const baseAmount = baseCents / 100;
  const effectiveRate = parsed.quoteCurrency === 'USD' ? 1 : parsed.exchangeRate;
  // Scale through minor units before rounding so binary float drift (e.g. 19.99 * 36.5) cannot skew ledger totals.
  const convertedAmount = Math.round(baseCents * effectiveRate) / 100;

  return Object.freeze({
    baseCurrency: 'USD',
    baseAmount,
    quoteCurrency: parsed.quoteCurrency,
    exchangeRate: effectiveRate,
    convertedAmount,
    capturedAt: parsed.capturedAt ?? new Date().toISOString(),
  });
}
