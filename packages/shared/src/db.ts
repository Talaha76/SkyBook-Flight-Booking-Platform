import { Pool, PoolClient } from 'pg';
import { Logger } from './logger';

export function createPool(connectionString: string, max = 10): Pool {
  return new Pool({ connectionString, max, idleTimeoutMillis: 30_000 });
}

/** Runs fn inside a transaction; commits on success, rolls back on error. */
export async function withTx<T>(pool: Pool, fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

/** Tables every event-driven service needs: the transactional outbox and the consumer dedupe table. */
export const COMMON_MIGRATIONS: string[] = [
  `CREATE TABLE IF NOT EXISTS outbox_events (
     id              BIGSERIAL PRIMARY KEY,
     event_id        UUID UNIQUE NOT NULL,
     aggregate_id    UUID NOT NULL,
     topic           TEXT NOT NULL,
     event_key       TEXT NOT NULL,
     payload         JSONB NOT NULL,
     headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
     status          TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','PUBLISHED')),
     attempt_count   INT  NOT NULL DEFAULT 0,
     last_error      TEXT,
     next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
     created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
     published_at    TIMESTAMPTZ
   )`,
  `CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events (status, next_attempt_at, id)`,
  `CREATE TABLE IF NOT EXISTS processed_events (
     consumer     TEXT NOT NULL,
     event_id     UUID NOT NULL,
     processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
     PRIMARY KEY (consumer, event_id)
   )`,
];

/**
 * Minimal forward-only migration runner. Each statement is applied once, in order,
 * guarded by an advisory lock so several replicas can start at the same time.
 */
export async function runMigrations(pool: Pool, name: string, statements: string[], log?: Logger) {
  await withTx(pool, async (c) => {
    await c.query(`SELECT pg_advisory_xact_lock(hashtext($1))`, [`migrations:${name}`]);
    await c.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
      name TEXT NOT NULL, idx INT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (name, idx))`);
    const { rows } = await c.query<{ idx: number }>(`SELECT idx FROM schema_migrations WHERE name = $1`, [name]);
    const done = new Set(rows.map((r) => r.idx));
    for (let i = 0; i < statements.length; i++) {
      if (done.has(i)) continue;
      await c.query(statements[i]);
      await c.query(`INSERT INTO schema_migrations (name, idx) VALUES ($1, $2)`, [name, i]);
    }
  });
  log?.info({ migrations: name }, 'migrations applied');
}

/** Retries connecting at startup so services survive `docker compose up` ordering. */
export async function waitForDb(pool: Pool, log: Logger, attempts = 30) {
  for (let i = 1; ; i++) {
    try {
      await pool.query('SELECT 1');
      return;
    } catch (err) {
      if (i >= attempts) throw err;
      log.warn({ attempt: i, err: (err as Error).message }, 'database not ready, retrying');
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}
