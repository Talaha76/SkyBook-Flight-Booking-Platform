import { SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import { makeEvent } from '../../../packages/shared/src';
import { contractName, toSqsRecord } from './contracts';
import { ForwardError, SqsSender, forwardBatch } from './forwarder';

const flightId = '9b2c0a40-1111-4111-8111-111111111111';
const bookingId = '7a1b0c20-2222-4222-8222-222222222222';
const rec = (n: number) => toSqsRecord(makeEvent('BookingConfirmed', bookingId, { bookingId, flightId, n }));

describe('contracts', () => {
  test('versioned contract names', () => {
    expect(contractName('BookingConfirmed')).toBe('skybook.booking.confirmed.v1');
    expect(contractName('SeatHoldFailed')).toBe('skybook.seat.hold_failed.v1');
  });
  test('payment tokens never leave Kafka; eventId is an SQS attribute', () => {
    const r = toSqsRecord(makeEvent('BookingRequested', bookingId, { bookingId, flightId, paymentToken: 'tok_success' }, 'req-1'));
    expect(r.body).not.toContain('tok_success');
    expect(r.attributes).toEqual({ eventId: r.eventId, contract: 'skybook.booking.requested.v1', flightId, requestId: 'req-1' });
  });
  test('empty requestId is not sent (SQS rejects empty attribute values)', () => {
    expect(toSqsRecord(makeEvent('SeatHeld', bookingId, { bookingId, flightId })).attributes.requestId).toBeUndefined();
  });
});

function fakeSqs(script: Array<(ids: string[]) => { Successful?: any[]; Failed?: any[] } | Error>) {
  const calls: string[][] = [];
  const sender: SqsSender = {
    async send(cmd: SendMessageBatchCommand) {
      const ids = (cmd.input.Entries ?? []).map((e) => e.Id!);
      calls.push(ids);
      const step = script[Math.min(calls.length - 1, script.length - 1)](ids);
      if (step instanceof Error) throw step;
      return step;
    },
  };
  return { sender, calls };
}

describe('forwardBatch', () => {
  test('all succeed in one call', async () => {
    const { sender, calls } = fakeSqs([(ids) => ({ Successful: ids.map((Id) => ({ Id })) })]);
    const r = await forwardBatch(sender, 'q', [rec(1), rec(2), rec(3)], { baseDelayMs: 1 });
    expect(r).toEqual({ forwarded: 3, rejected: [], retriedEntries: 0 });
    expect(calls).toHaveLength(1);
  });

  test('only entries in Failed[] are retried', async () => {
    const { sender, calls } = fakeSqs([
      (ids) => ({ Successful: [{ Id: ids[0] }, { Id: ids[2] }], Failed: [{ Id: ids[1], SenderFault: false, Code: 'InternalError' }] }),
      (ids) => ({ Successful: ids.map((Id) => ({ Id })) }),
    ]);
    const r = await forwardBatch(sender, 'q', [rec(1), rec(2), rec(3)], { baseDelayMs: 1 });
    expect(r.forwarded).toBe(3);
    expect(calls).toEqual([['m0', 'm1', 'm2'], ['m1']]);
  });

  test('sender-fault entries are reported, not retried', async () => {
    const { sender, calls } = fakeSqs([(ids) => ({ Successful: [{ Id: ids[0] }], Failed: [{ Id: ids[1], SenderFault: true, Code: 'InvalidParameterValue' }] })]);
    const r = await forwardBatch(sender, 'q', [rec(1), rec(2)], { baseDelayMs: 1 });
    expect(r.forwarded).toBe(1);
    expect(r.rejected).toHaveLength(1);
    expect(calls).toHaveLength(1);
  });

  test('persistent outage throws ForwardError so the offset is NOT committed', async () => {
    const { sender, calls } = fakeSqs([() => new Error('ECONNREFUSED')]);
    await expect(forwardBatch(sender, 'q', [rec(1)], { maxAttempts: 3, baseDelayMs: 1 })).rejects.toBeInstanceOf(ForwardError);
    expect(calls).toHaveLength(3);
  });

  test('refuses batches over the SQS limit of 10', async () => {
    const { sender } = fakeSqs([(ids) => ({ Successful: ids.map((Id) => ({ Id })) })]);
    await expect(forwardBatch(sender, 'q', Array.from({ length: 11 }, (_, i) => rec(i)))).rejects.toThrow('batch too large');
  });
});
