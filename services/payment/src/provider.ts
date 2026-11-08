/**
 * Mock payment provider. No card data is ever handled - only fake tokens:
 *   tok_success -> always succeeds
 *   tok_fail    -> always declined
 *   anything    -> fails with probability failureRate
 */
export type PaymentOutcome = { ok: true } | { ok: false; reason: string };

export function decidePayment(args: { token: string; expiresAt: Date; failureRate: number; now?: Date; random?: () => number }): PaymentOutcome {
  const now = args.now ?? new Date();
  if (args.expiresAt.getTime() <= now.getTime()) return { ok: false, reason: 'HOLD_EXPIRED' };
  if (args.token === 'tok_fail') return { ok: false, reason: 'CARD_DECLINED' };
  if (args.token === 'tok_success') return { ok: true };
  const rnd = (args.random ?? Math.random)();
  return rnd < args.failureRate ? { ok: false, reason: 'PROVIDER_ERROR' } : { ok: true };
}
