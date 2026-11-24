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

export function freezeCurrencyTx(input: z.input<typeof CurrencyInputSchema>): MonetarySnapshot {
  const parsed = CurrencyInputSchema.parse(input);
  const baseCents = Math.round(parsed.amountUsd * 100);
  const effectiveRate = parsed.quoteCurrency === 'USD' ? 1 : parsed.exchangeRate;

  // Scale through minor units before rounding so binary float drift (e.g. 19.99 * 36.5) cannot skew ledger totals.
  return Object.freeze({
    baseCurrency: 'USD',
    baseAmount: baseCents / 100,
    quoteCurrency: parsed.quoteCurrency,
    exchangeRate: effectiveRate,
    convertedAmount: Math.round(baseCents * effectiveRate) / 100,
    capturedAt: parsed.capturedAt ?? new Date().toISOString(),
  });
}
