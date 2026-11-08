import pino from 'pino';

/** JSON structured logger. Child loggers add requestId / bookingId / eventId to every line. */
export function createLogger(service: string) {
  return pino({
    level: process.env.LOG_LEVEL ?? 'info',
    base: { service },
    timestamp: pino.stdTimeFunctions.isoTime,
    redact: ['req.headers.authorization', '*.paymentToken', 'paymentToken'],
  });
}

export type Logger = ReturnType<typeof createLogger>;
