import type Redis from 'ioredis';
import { Pool } from 'pg';
import { z } from 'zod';
import { Logger, promClient, registry, sleep } from '../../../packages/shared/src';

export const cacheHits = new promClient.Counter({ name: 'cache_hits_total', help: 'Search cache hits', registers: [registry] });
export const cacheMisses = new promClient.Counter({ name: 'cache_misses_total', help: 'Search cache misses', registers: [registry] });

export const SearchQuery = z.object({
  origin: z.string().trim().toUpperCase().regex(/^[A-Z]{3}$/, 'origin must be a 3-letter IATA code'),
  destination: z.string().trim().toUpperCase().regex(/^[A-Z]{3}$/, 'destination must be a 3-letter IATA code'),
  date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'date must be YYYY-MM-DD'),
});
export type SearchQuery = z.infer<typeof SearchQuery>;

export const cacheKey = (q: { origin: string; destination: string; date: string }) => `search:${q.origin}:${q.destination}:${q.date}`;

export function toFlightDto(r: any) {
  return {
    id: r.id,
    flightNumber: r.flight_number,
    airline: r.airline,
    origin: r.origin,
    destination: r.destination,
    departureDate: typeof r.departure_date === 'string' ? r.departure_date : formatDate(r.departure_date),
    departureTime: new Date(r.departure_time).toISOString(),
    arrivalTime: new Date(r.arrival_time).toISOString(),
    priceCents: r.price_cents,
    totalSeats: r.total_seats,
    seatsAvailable: r.seats_available,
  };
}

function formatDate(d: Date) {
  // pg returns DATE as a local-midnight Date; format with local parts to avoid TZ shifts.
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

export async function queryFlights(pool: Pool, q: SearchQuery) {
  const { rows } = await pool.query(
    `SELECT id, flight_number, airline, origin, destination, to_char(departure_date, 'YYYY-MM-DD') AS departure_date,
            departure_time, arrival_time, price_cents, total_seats, seats_available
       FROM flights
      WHERE origin = $1 AND destination = $2 AND departure_date = $3
      ORDER BY departure_time
      LIMIT 100`,
    [q.origin, q.destination, q.date],
  );
  return rows.map(toFlightDto);
}

/**
 * Cache-aside search with stampede protection (build guide 3.7):
 * hit -> return; miss -> one caller takes a short SET NX PX lock and rebuilds, others wait briefly.
 * Any Redis failure falls back to Postgres so the cache can never take search down.
 */
export function createSearch(deps: { pool: Pool; redis: Redis | null; log: Logger; ttlSeconds: number; enabled: boolean }) {
  const { pool, redis, log } = deps;
  return async function search(q: SearchQuery, opts: { bypassCache?: boolean } = {}): Promise<{ flights: unknown[]; cache: 'HIT' | 'MISS' | 'BYPASS' }> {
    if (!deps.enabled || !redis || opts.bypassCache) return { flights: await queryFlights(pool, q), cache: 'BYPASS' };
    const key = cacheKey(q);
    try {
      const cached = await redis.get(key);
      if (cached) {
        cacheHits.inc();
        return { flights: JSON.parse(cached), cache: 'HIT' };
      }
      cacheMisses.inc();
      const lockKey = `lock:${key}`;
      const gotLock = await redis.set(lockKey, '1', 'PX', 2000, 'NX');
      if (!gotLock) {
        for (let i = 0; i < 20; i++) {
          await sleep(50);
          const v = await redis.get(key);
          if (v) return { flights: JSON.parse(v), cache: 'HIT' };
        }
        return { flights: await queryFlights(pool, q), cache: 'MISS' };
      }
      try {
        const flights = await queryFlights(pool, q);
        await redis.set(key, JSON.stringify(flights), 'EX', deps.ttlSeconds);
        return { flights, cache: 'MISS' };
      } finally {
        await redis.del(lockKey).catch(() => undefined);
      }
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'cache unavailable, falling back to database');
      return { flights: await queryFlights(pool, q), cache: 'BYPASS' };
    }
  };
}
