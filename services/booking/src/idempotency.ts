import { createHash } from 'crypto';
import { Pool } from 'pg';

/** Deterministic JSON (sorted keys) so the same logical body always hashes the same. */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  const obj = value as Record<string, unknown>;
  return `{${Object.keys(obj).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson(obj[k])}`).join(',')}}`;
}

/** SHA-256 of method + path + user + body. Including the user means a key can't be replayed by someone else. */
export function hashRequest(method: string, path: string, userId: string, body: unknown): string {
  return createHash('sha256').update(`${method.toUpperCase()}\n${path}\n${userId}\n${canonicalJson(body)}`).digest('hex');
}

export interface IdempotencyRow {
  key: string;
  user_id: string;
  request_hash: string;
  status: 'IN_PROGRESS' | 'COMPLETED';
  response_code: number | null;
  response_body: unknown;
  created_at: Date;
}

export interface IdempotencyRepo {
  /** INSERT ... ON CONFLICT DO NOTHING; true when this caller now owns the key. */
  tryInsert(key: string, userId: string, hash: string): Promise<boolean>;
  find(key: string): Promise<IdempotencyRow | null>;
  /** Re-claims an IN_PROGRESS key whose owner probably crashed (older than staleSeconds). */
  takeOverStale(key: string, hash: string, staleSeconds: number): Promise<boolean>;
  /** Deletes an IN_PROGRESS key after the request failed, so the client can retry. */
  release(key: string): Promise<void>;
}

export type Decision =
  | { kind: 'proceed' }
  | { kind: 'replay'; code: number; body: unknown }
  | { kind: 'in_progress' }
  | { kind: 'mismatch' };

/**
 * The unique constraint on idempotency_keys.key is the lock: of N identical concurrent
 * requests exactly one insert succeeds; the rest see the existing row.
 */
export async function beginIdempotent(
  repo: IdempotencyRepo,
  key: string,
  userId: string,
  hash: string,
  staleSeconds = 30,
): Promise<Decision> {
  if (await repo.tryInsert(key, userId, hash)) return { kind: 'proceed' };
  const existing = await repo.find(key);
  if (!existing) {
    // Row vanished between insert and select (expired or released) - try once more.
    return (await repo.tryInsert(key, userId, hash)) ? { kind: 'proceed' } : { kind: 'in_progress' };
  }
  if (existing.request_hash !== hash) return { kind: 'mismatch' };
  if (existing.status === 'COMPLETED') return { kind: 'replay', code: existing.response_code ?? 200, body: existing.response_body };
  if (await repo.takeOverStale(key, hash, staleSeconds)) return { kind: 'proceed' };
  return { kind: 'in_progress' };
}

export function pgIdempotencyRepo(pool: Pool): IdempotencyRepo {
  return {
    async tryInsert(key, userId, hash) {
      const r = await pool.query(
        `INSERT INTO idempotency_keys (key, user_id, request_hash, status) VALUES ($1, $2, $3, 'IN_PROGRESS')
         ON CONFLICT (key) DO NOTHING RETURNING key`,
        [key, userId, hash],
      );
      return r.rowCount === 1;
    },
    async find(key) {
      const r = await pool.query<IdempotencyRow>(`SELECT * FROM idempotency_keys WHERE key = $1`, [key]);
      return r.rows[0] ?? null;
    },
    async takeOverStale(key, hash, staleSeconds) {
      const r = await pool.query(
        `UPDATE idempotency_keys SET created_at = now()
          WHERE key = $1 AND request_hash = $2 AND status = 'IN_PROGRESS' AND created_at < now() - make_interval(secs => $3)`,
        [key, hash, staleSeconds],
      );
      return r.rowCount === 1;
    },
    async release(key) {
      await pool.query(`DELETE FROM idempotency_keys WHERE key = $1 AND status = 'IN_PROGRESS'`, [key]);
    },
  };
}

export async function deleteExpiredKeys(pool: Pool): Promise<number> {
  const r = await pool.query(`DELETE FROM idempotency_keys WHERE expires_at < now()`);
  return r.rowCount ?? 0;
}
