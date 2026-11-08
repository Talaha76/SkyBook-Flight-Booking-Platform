import { Pool } from 'pg';
import { createLogger, createPool, runMigrations } from '../../packages/shared/src';
import { BOOKING_MIGRATIONS } from '../../services/booking/src/migrations';
import { INVENTORY_MIGRATIONS } from '../../services/inventory/src/migrations';

export const log = createLogger('itest');
log.level = 'silent';

let container: { stop: () => Promise<unknown> } | undefined;

/**
 * Uses TEST_DATABASE_URL when set (e.g. the Compose Postgres), otherwise starts a throwaway
 * PostgreSQL with Testcontainers (requires Docker).
 */
export async function setupDb(): Promise<Pool> {
  let url = process.env.TEST_DATABASE_URL;
  if (!url) {
    const { PostgreSqlContainer } = await import('@testcontainers/postgresql');
    const c = await new PostgreSqlContainer('postgres:16-alpine').start();
    container = c;
    url = c.getConnectionUri();
  }
  const pool = createPool(url, 60);
  await runMigrations(pool, 'booking', BOOKING_MIGRATIONS);
  await runMigrations(pool, 'inventory', INVENTORY_MIGRATIONS);
  return pool;
}

export async function resetDb(pool: Pool) {
  await pool.query(`TRUNCATE bookings, idempotency_keys, outbox_events, processed_events, seat_holds, seat_inventory CASCADE`);
}

export async function teardownDb(pool: Pool) {
  await pool.end();
  await container?.stop();
}
