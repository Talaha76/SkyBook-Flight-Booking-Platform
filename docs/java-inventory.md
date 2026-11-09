# Inventory service in Java / Spring Boot

`services/inventory-java` is a second implementation of the Inventory service: Java 21 and Spring Boot 3.2.
It is a **drop-in replacement**. The Node version (`services/inventory`) stays, and either one can run against the
same stack without changing anything else:

| Contract | Node | Java |
|---|---|---|
| Port, env vars | `PORT`, `DATABASE_URL`, `KAFKA_BROKERS`, `INTERNAL_TOKEN`, `INVENTORY_LOCK_STRATEGY`, `SEAT_HOLD_TTL_SECONDS`, `KAFKA_PARTITIONS_CONCURRENCY` | same (Node-style `postgres://` URL converted to JDBC in `DatabaseUrl`) |
| Tables | `seat_inventory`, `seat_holds`, `outbox_events`, `processed_events` | same schema; Flyway `V1__init.sql` is all `IF NOT EXISTS` with `baseline-on-migrate`, so it runs on a database the Node service already created |
| Kafka in | `booking.events` (`BookingRequested`), `payment.events` (`PaymentSucceeded`, `PaymentFailed`), consumer group `inventory` | same group, so offsets carry over when you swap |
| Kafka out | `inventory.events`: `SeatHeld`, `SeatHoldFailed`, `SeatConfirmed`, `SeatReleased`; key = flightId; headers `x-request-id`, `event-type`, `x-message-id` | same envelope (`eventId, type, version, occurredAt, aggregateId, requestId?, payload`); `requestId` is omitted, never `null`, because the Node consumers validate with zod |
| HTTP | `GET /inventory/{flightId}` (AGENT only, internal token), `/health`, `/ready`, `/metrics` | same responses and status codes (401/403/400/404) |
| Metrics | `http_request_duration_seconds`, `kafka_messages_consumed_total`, `kafka_consumer_lag`, `outbox_pending_total`, `outbox_published_total` | same Prometheus names via Micrometer, plus JVM metrics |

## How the guarantees map to Spring

| Guarantee | Node | Java |
|---|---|---|
| No overbooking | `SELECT ... FOR UPDATE` or versioned `UPDATE` with retries (`holds.ts`) | same SQL in `SeatHoldRepository` (JdbcTemplate, no JPA, so the SQL is visible and identical) |
| One transaction per event: dedupe + seat change + outbox row | `processEventOnce` + `withTx` | `@Transactional InventoryEventService.handleOnce`, called from the listener **in another bean**, so the call goes through the Spring proxy (a self-call would silently skip the transaction) |
| At-least-once, ack only after commit | KafkaJS `eachMessage` + manual commit | Spring Kafka `AckMode.MANUAL_IMMEDIATE`; `ack.acknowledge()` runs after `handleOnce` returns, i.e. after the commit |
| Never skip a record because the DB is down | consumer retry loop | `DefaultErrorHandler` with exponential backoff (0.5 s → 30 s, no max elapsed time); malformed messages are acked and skipped so they can't block a partition |
| Duplicate-safe | `processed_events (consumer, event_id)` | same table, same `consumer = 'inventory'` key |
| Transactional outbox | poller with `FOR UPDATE SKIP LOCKED`, global backoff | `@Scheduled OutboxPublisher` (200 ms), same query; the attempt counter is committed **before** the error is rethrown, so backoff really advances |
| Hold expiry | `setInterval` sweeper | `@Scheduled SweeperSchedule` → `@Transactional ExpiredHoldSweeper.sweep` (`FOR UPDATE SKIP LOCKED`, safe with several replicas) |
| JSON logs with request id | pino | Logback + logstash encoder, `requestId` in MDC, `impl=java` field |

## Tests

| Test | Kind | What it shows |
|---|---|---|
| `InventoryEventServiceTest` | unit (Mockito) | every handler outcome: held, sold out, existing hold, duplicate, confirm, release, unknown type, invalid payload |
| `EnvelopeParserTest` | unit | envelope validation equivalent to the zod schema; outgoing events omit a null `requestId` |
| `DatabaseUrlTest`, `InternalAuthFilterTest` | unit | URL conversion; 401 without token, open health paths, role handling |
| `OverbookingIT` | integration (Postgres) | **200 concurrent holds on 150 seats → exactly 150 held / 50 SOLD_OUT** for both `PESSIMISTIC` and `OPTIMISTIC` |
| `ConsumerSafetyIT` | integration (Postgres) | same event twice → one change; PaymentFailed releases once; PaymentSucceeded confirms; expired holds swept; HTTP contract incl. `/metrics` names |
| `KafkaRoundTripIT` | integration (Postgres + Kafka) | BookingRequested (sent twice) → exactly one `SeatHeld` on `inventory.events`; PaymentFailed → `SeatReleased`; outbox drained |

Integration tests use `TEST_DATABASE_URL` / `TEST_KAFKA_BROKERS` when set (the Compose stack), otherwise Testcontainers.
They use their own topic names and consumer group, so they never touch the live topics.

## Commands (PowerShell, from the repo root)

```powershell
# 1. Build the Java image and swap it in (the rest of the stack keeps running)
docker compose -f docker-compose.yml -f docker-compose.java.yml up --build -d

# 2. Check that it's the Java service answering
docker compose -f docker-compose.yml -f docker-compose.java.yml ps inventory
docker compose -f docker-compose.yml -f docker-compose.java.yml logs inventory --tail 20   # JSON lines with "impl":"java"

# 3. Drop-in proof: the unchanged end-to-end suite
docker compose -f docker-compose.yml -f docker-compose.java.yml --profile tools run --rm e2e

# 4. Java unit + integration tests (mvn verify) against this stack's Postgres and Kafka
docker compose -f docker-compose.yml -f docker-compose.java.yml --profile tools run --rm tests-java

# 5. Optional: same k6 booking burst on both implementations
docker compose -f docker-compose.yml -f docker-compose.java.yml --profile tools run --rm k6 run /scripts/booking-burst.js
docker compose up -d --force-recreate inventory          # back to Node
docker compose --profile tools run --rm k6 run /scripts/booking-burst.js
```

Kubernetes (kind):

```powershell
docker build -t skybook-inventory-java:latest services/inventory-java
kind load docker-image skybook-inventory-java:latest --name skybook
helm upgrade skybook deploy/helm/skybook -n skybook --reuse-values --set inventory.implementation=java
kubectl -n skybook get pods -l app.kubernetes.io/name=inventory --show-labels   # skybook.io/impl=java
```

`inventory.implementation=node` switches back. The Java pod gets a startup probe and 768 Mi memory limit
(`inventory.javaResources`); the heap is sized from the container limit (`-XX:MaxRAMPercentage=75`).

## Status

Verified on the owner's machine (2026-11-08): `mvn verify` → unit 21/21 and integration 8/8 passed against the Compose
Postgres and Kafka (pessimistic 150/50 in 618 ms, optimistic 150/50 in 479 ms), and the unchanged e2e suite passed
**23/23** with the Java service in place of the Node one. Still to run: the Helm toggle on kind, the CI job, and the
optional k6 comparison.
