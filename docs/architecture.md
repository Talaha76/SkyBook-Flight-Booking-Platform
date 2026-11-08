# Architecture and design decisions

## Topology

```
client ──▶ API Gateway :8080 (JWT, roles, Redis rate limit, X-Request-Id, Swagger UI)
              │ HTTP (x-internal-token, x-user-id, x-user-role)
   ┌──────────┼───────────────┬──────────────┬──────────────┐
   ▼          ▼               ▼              ▼              ▼
flight-search booking       inventory      payment      notification
 (flights DB, (bookings,    (seat_inventory,(payments)   (notifications)
  Redis)      idempotency,   seat_holds)
              outbox)
       ▲          │ outbox        ▲ │ outbox     ▲ │ outbox     ▲ │ outbox (retry/DLQ)
       │          ▼               │ ▼            │ ▼            │ ▼
       └───────────────────── Kafka (KRaft) ─────────────────────────┘
     booking.events · inventory.events · payment.events · notification.retry.{1m,5m,30m} · notification.dlq
```

Every service owns its database (one PostgreSQL server, one database per service). No service reads
another service's tables. Seeding is the one exception: it writes the same flights into the Flight
Search read model and Inventory.

## Saga (choreography)

| Step | Producer → topic | Consumer reaction |
|---|---|---|
| 1 | Booking → `booking.events` **BookingRequested** | Inventory holds a seat (row lock or optimistic version check) |
| 2a | Inventory → `inventory.events` **SeatHeld** (price, TTL) | Booking → `SEAT_HELD`; Payment charges; Flight Search updates `seats_available` and evicts the cache key |
| 2b | Inventory → **SeatHoldFailed** | Booking → `REJECTED` → **BookingRejected** |
| 3a | Payment → `payment.events` **PaymentSucceeded** | Booking → `CONFIRMED` → **BookingConfirmed**; Inventory marks hold `CONFIRMED` |
| 3b | Payment → **PaymentFailed** | Inventory **releases the seat** (compensation) → **SeatReleased**; Booking → `CANCELLED` → **BookingCancelled** |
| 4 | Booking outcomes | Notification sends email (fake sink), with retries → DLQ |
| — | Inventory sweeper → **SeatReleased(EXPIRED)** | Booking → `CANCELLED` (payment crashed / never answered) |

All messages are keyed by **flightId**, so every event for a flight lands on the same partition in order.
The booking state machine (`services/booking/src/saga.ts`) also accepts a payment result while still
`PENDING`, because `inventory.events` and `payment.events` are different topics with no cross-topic ordering.

## Decisions (ADR-style)

1. **Transactional outbox in every producing service**, not only Booking. Inventory and Payment emit
   events as a result of consuming events. Writing `processed_events` + business change + outbox row in one
   transaction makes each step atomic: no dual writes anywhere.
2. **At-least-once delivery + idempotent consumers.** `processed_events (consumer, event_id)` dedupes per consumer group.
   Offsets are resolved only after the DB commit (`eachBatch`, manual `resolveOffset`).
3. **Outbox ordering.** On Kafka failure the publisher backs off as a whole instead of per-row, so a
   later event for the same flight can never overtake an earlier one.
4. **Idempotency key = lock.** `INSERT … ON CONFLICT DO NOTHING` on the primary key. The request hash
   includes the user id, so a key cannot be replayed by another user (→ 422). A stale `IN_PROGRESS` key
   (owner crashed) is taken over after 30 s; a failed request releases its key so the client can retry.
5. **Seat price lives in Inventory.** Booking cannot read the flights table, so the price travels on
   `SeatHeld.amountCents` and is charged by Payment and stored on the confirmed booking.
6. **Flight Search keeps a read model** of availability (`seats_available`), updated from `inventory.events`.
   The cache key is deleted *after* the DB commit (`afterCommit` hook) to avoid re-caching stale data.
7. **Stampede protection:** `SET lock:<key> NX PX 2000`; losers poll the cache for up to 1 s, then fall back to the DB.
8. **Gateway → service trust:** services are not published on the host; they require `x-internal-token`
   and take identity from gateway-set headers. In Kubernetes this becomes a NetworkPolicy plus mTLS.
9. **One shared PostgreSQL server** with a database per service (the build guide's 8 GB layout). On AWS
   each becomes its own RDS/Aurora instance (see the guide's blueprint table).
10. **Demo authentication:** the gateway issues HS256 JWTs for three demo users. A real deployment would
    use an IdP (Cognito / OIDC) with RS256 and JWKS; the gateway verification code is the only part that changes.
