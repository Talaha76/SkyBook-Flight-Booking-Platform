import { Logger } from '../../../packages/shared/src';

export interface Message {
  recipient: string;
  subject: string;
  body: string;
}

/** Builds the customer-facing message for a booking outcome event. */
export function renderMessage(type: string, p: { bookingId: string; passengerName: string; passengerEmail: string; reason?: string; amountCents?: number | null }): Message {
  const ref = p.bookingId.slice(0, 8).toUpperCase();
  switch (type) {
    case 'BookingConfirmed':
      return {
        recipient: p.passengerEmail,
        subject: `SkyBook booking ${ref} confirmed`,
        body: `Hi ${p.passengerName}, your booking ${ref} is confirmed. Amount charged: $${((p.amountCents ?? 0) / 100).toFixed(2)}.`,
      };
    case 'BookingCancelled':
      return { recipient: p.passengerEmail, subject: `SkyBook booking ${ref} cancelled`, body: `Hi ${p.passengerName}, booking ${ref} was cancelled (${p.reason ?? 'unknown reason'}). Your seat was released.` };
    default:
      return { recipient: p.passengerEmail, subject: `SkyBook booking ${ref} could not be completed`, body: `Hi ${p.passengerName}, booking ${ref} was rejected (${p.reason ?? 'unknown reason'}).` };
  }
}

/**
 * Fake email sink: logs the email. Fails for recipients containing "fail-notify" (to demo
 * retries + DLQ deterministically) or randomly at NOTIFICATION_FAILURE_RATE.
 */
export function createSender(log: Logger, failureRate: number) {
  return async function send(msg: Message): Promise<void> {
    if (msg.recipient.includes('fail-notify')) throw new Error('SMTP 550: mailbox unavailable (simulated)');
    if (Math.random() < failureRate) throw new Error('SMTP 421: service temporarily unavailable (simulated)');
    log.info({ to: msg.recipient, subject: msg.subject }, 'EMAIL SENT (fake sink)');
  };
}
