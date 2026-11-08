import { Logger } from './logger';

type Closer = () => Promise<unknown> | unknown;

/** Registers SIGINT/SIGTERM handlers that close resources in reverse order. */
export function onShutdown(log: Logger, closers: Closer[]) {
  let closing = false;
  const handler = async (signal: string) => {
    if (closing) return;
    closing = true;
    log.info({ signal }, 'shutting down');
    for (const close of [...closers].reverse()) {
      try { await close(); } catch (err) { log.warn({ err: (err as Error).message }, 'error during shutdown'); }
    }
    process.exit(0);
  };
  process.on('SIGINT', () => void handler('SIGINT'));
  process.on('SIGTERM', () => void handler('SIGTERM'));
}

/** Logs fatal startup errors and exits non-zero so the orchestrator restarts the container. */
export function runMain(log: Logger, main: () => Promise<void>) {
  main().catch((err) => {
    log.fatal({ err }, 'fatal startup error');
    process.exit(1);
  });
}
