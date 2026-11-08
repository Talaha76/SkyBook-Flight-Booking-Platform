# SkyBook: Flight Booking Microservices

A flight-booking back end built as **six Node.js/TypeScript microservices** that talk over **Apache Kafka**, plus a **Kafka → AWS SQS bridge**.
Each service has its own **PostgreSQL** database, flight search is cached in **Redis**, and the stack runs
locally with **Docker Compose**, including **Prometheus/Grafana** monitoring. It deploys to Kubernetes with a **Helm** chart and has an
**AWS CDK blueprint** (MSK, RDS, ElastiCache, EKS, SQS, WAF) with a cost estimate.

It covers the patterns that matter in real booking systems: an **idempotent booking API**, a
**transactional outbox**, a **choreographed saga with compensation**, **duplicate-safe consumers**,
**overbooking prevention** (pessimistic and optimistic locking), **cache-aside with stampede protection and
event-driven invalidation**, and **retry topics with a dead-letter queue and replay tool**.

> Every number in this README and in `docs/` was measured. See **[docs/results-2026-11-07.md](docs/results-2026-11-07.md)** (owner's machine + real AWS SQS)
> and [docs/benchmarks.md](docs/benchmarks.md) (development runs).

## Headline results (measured)

| What | Result |
|---|---|
| Flight search, 1M rows, k6 p95 | **1,021 ms → 28 ms** with a composite index → **25.5 ms** with Redis (89 % hit rate, 1,641 req/s) |
| Idempotency, 100 identical concurrent requests | **1 booking, 0 duplicates** |
| 10× booking spike (5/s → 50/s) | 3,575 bookings, 17,875 events, **0 lost**, 0 % errors, consumers caught up in 161 s |
| Kafka → **real Amazon SQS**, 2-minute network outage | **1,200 / 1,200 events delivered**, backlog drained in 21 s |
| Overbooking, 200 concurrent holds on 150 seats | exactly **150 held / 50 rejected** (pessimistic and optimistic locking) |
| Kubernetes (kind + Helm) | 13 pods Running, **e2e 23/23 passed** against the cluster |

## Architecture

```
client ─▶ API Gateway :8080 ── JWT · roles · Redis rate limit · X-Request-Id · Swagger UI (/docs)
              │
   ┌──────────┼──────────────┬───────────────┬──────────────┐
   ▼          ▼              ▼               ▼              ▼
flight-search booking      inventory       payment (mock) notification
 Postgres +   Postgres:    Postgres:       Postgres       Postgres
 Redis cache  bookings,    seat_inventory, payments       notifications
 read model   idempotency, seat_holds                     retry + DLQ
              outbox
        ▲            │ outbox    ▲ │ outbox    ▲ │ outbox    ▲ │
        └────────────┴──────── Kafka (KRaft) ─┴────────────┴─┘
   booking.events · inventory.events · payment.events · notification.retry.{1m,5m,30m} · notification.dlq
                      (all keyed by flightId → per-flight ordering)
        │
        └─▶ aws-bridge ── SendMessageBatch (commit offset only after AWS ack, pause on outage) ──▶ Amazon SQS
                          versioned contracts: skybook.booking.confirmed.v1, skybook.seat.held.v1 …
```

**Booking saga:** `BookingRequested → SeatHeld → PaymentSucceeded → CONFIRMED`. If payment fails, the flow becomes
`PaymentFailed → seat released (compensation) → CANCELLED`. If no seat can be held, the booking ends at `SeatHoldFailed → REJECTED`.
A sweeper releases holds whose 10-minute TTL has passed, so a crashed payment never strands a seat.
Design decisions are in [docs/architecture.md](docs/architecture.md).

| Service | Port (internal) | Owns |
|---|---|---|
| gateway | **8080 (published)** | JWT auth, roles, rate limiting, routing, OpenAPI |
| flight-search | 3001 | `flights`, `airports`, Redis search cache |
| booking | 3002 | `bookings`, `idempotency_keys`, `outbox_events` |
| inventory | 3003 | `seat_inventory`, `seat_holds` |
| payment | 3004 | `payments` (mock provider) |
| notification | 3005 | `notifications`, retry topics, DLQ |
| aws-bridge (profile `aws`) | 3006 | Kafka offsets only (the checkpoint); forwards to SQS |

## Run it (Windows PowerShell)

**Prerequisite:** Docker Desktop running, with about 6 GB of memory available (Settings → Resources). Nothing else is required: Node, Kafka and Postgres all run in containers.

```powershell
# 1. Unzip and enter the folder
Expand-Archive .\skybook.zip -DestinationPath . ; cd .\skybook

# 2. Build and start everything (the first run downloads images; allow a few minutes)
docker compose up --build -d

# 3. Wait until every service is healthy (gateway shows "healthy")
docker compose ps

# 4. End-to-end check: search, booking, idempotency, saga, compensation, auth
docker compose --profile tools run --rm e2e

# 5. Unit + integration tests (real PostgreSQL)
docker compose --profile tools run --rm tests
```

Expected end of step 4: `ALL CHECKS PASSED`.

| Open | URL |
|---|---|
| Swagger UI (try every endpoint) | http://localhost:8080/docs |
| Grafana dashboard "SkyBook Overview" | http://localhost:3000 |
| Prometheus (targets, alerts) | http://localhost:9090 |

Stop with `docker compose down`, or `docker compose down -v` to also delete the database volume.

### Try it by hand (PowerShell)

```powershell
$login = Invoke-RestMethod -Method Post -Uri http://localhost:8080/auth/login -ContentType 'application/json' -Body '{"username":"alice","password":"skybook"}'
$h = @{ Authorization = "Bearer $($login.accessToken)" }
$date = (Get-Date).AddDays(2).ToString('yyyy-MM-dd')
$flights = Invoke-RestMethod "http://localhost:8080/flights/search?origin=YYZ&destination=YVR&date=$date"
$flights.flights | Select-Object flightNumber, departureTime, priceCents, seatsAvailable

$key = [guid]::NewGuid().ToString()
$body = @{ flightId = $flights.flights[0].id; passengerName = 'Alice Martin'; passengerEmail = 'alice@example.com'; paymentToken = 'tok_success' } | ConvertTo-Json
$booking = Invoke-RestMethod -Method Post -Uri http://localhost:8080/bookings -Headers ($h + @{ 'Idempotency-Key' = $key }) -ContentType 'application/json' -Body $body
Start-Sleep 2
Invoke-RestMethod "http://localhost:8080/bookings/$($booking.id)" -Headers $h      # status: CONFIRMED
```

Demo users (password `skybook`): `alice`, `bob` (CUSTOMER), and `agent` (AGENT; can read inventory, payments and notifications).
Payment tokens: `tok_success` always succeeds and `tok_fail` is always declined. Any other token fails randomly at `PAYMENT_FAILURE_RATE` (10 %).
To watch retries and the dead-letter queue, use a passenger email containing `fail-notify`. The message lands in `notification.dlq` after about 30 s, then run:

```powershell
docker compose --profile tools run --rm dlq-replay                      # dry run, last hour
docker compose --profile tools run --rm dlq-replay --since 1h           # actually replay
```

### Kafka → AWS SQS bridge

```powershell
docker compose --profile aws up -d                                           # bridge + local fake SQS (no AWS account)
docker compose --profile aws --profile tools run --rm bridge-outage-test     # 2-min AWS outage: produced == received?
```

For real AWS (free tier), see [docs/aws-setup.md](docs/aws-setup.md). For Kubernetes (kind + Helm), see [docs/kubernetes.md](docs/kubernetes.md).
For the AWS production blueprint: `cd infra/aws-blueprint; npm ci; npm test` (synth + 12 checks); costs are in [docs/aws-cost-estimate.md](docs/aws-cost-estimate.md).

### Get your resume numbers (one command)

```powershell
.\scripts\run-benchmarks.ps1               # ~15 min: 1M-row seed, EXPLAIN, k6 search x3, idempotency race, 10x spike
.\scripts\run-benchmarks.ps1 -WithBridge   # + the 2-minute bridge outage test (fake SQS)
```

It writes `load-tests\results\RESULTS.md`: result tables plus the build guide's resume bullets, filled in with **your** measurements.
If PowerShell blocks the script, run it once with
`powershell -ExecutionPolicy Bypass -File .\scripts\run-benchmarks.ps1`.

### Load tests (k6, runs in a container)

```powershell
docker compose --profile tools run --rm k6 run /scripts/search.js
docker compose --profile tools run --rm k6 run /scripts/idempotency-race.js
docker compose --profile tools run --rm k6 run /scripts/booking-burst.js
```

### Developing without Docker for the services

```powershell
npm ci; npm run build
docker compose up -d postgres redis kafka   # infrastructure only
$env:TEST_DATABASE_URL = 'postgres://postgres:postgres@localhost:5432/skybook_test'; npm test
```

## Key implementations

| Feature | Where | Proof |
|---|---|---|
| Idempotent `POST /bookings` (key = lock, saved response replay, 409/422) | `services/booking/src/idempotency.ts`, `bookings.ts` | 50 (integration) and 100 (k6) concurrent identical requests → **1 booking** |
| Transactional outbox, `FOR UPDATE SKIP LOCKED`, exponential backoff | `packages/shared/src/outbox.ts` | Kafka stopped mid-booking → 201 returned, confirmed 7 s after Kafka returned |
| Duplicate-safe consumers (`processed_events`) | `packages/shared/src/consumer.ts` | same event twice → state changes once |
| Saga + compensation | `services/booking/src/saga.ts`, `services/inventory/src/handlers.ts` | e2e: `tok_fail` → CANCELLED, seat restored |
| No overbooking (row lock **and** optimistic version) | `services/inventory/src/holds.ts` | 200 concurrent holds / 150 seats → **150 / 50** for both |
| Composite index | `services/flight-search/src/migrations.ts` | 1M rows: k6 p95 **1,021 → 28 ms**; `EXPLAIN ANALYZE` 73 → 0.28 ms |
| Redis cache-aside, `SET NX PX` stampede lock, invalidation on `inventory.events` | `services/flight-search/src/search.ts` | e2e: second search `X-Cache: HIT`; availability updates after booking |
| Retry topics 1m/5m/30m → DLQ → replay tool | `services/notification/src`, `scripts/dlq-replay.ts` | failing recipient → `DEAD` after 3 retries; replay lists it |
| JWT + roles, zod validation, internal service token | `services/gateway/src`, `packages/shared/src/http.ts` | e2e: 401 / 403 checks |
| Metrics, JSON logs, request IDs carried through Kafka | `packages/shared/src/metrics.ts`, `logger.ts` | one `requestId` appears on all 6 services' log lines |
| Kafka → SQS bridge: commit-after-ack, `Failed[]` retry, pause/resume circuit breaker | `services/aws-bridge/src` | real SQS, 2-minute network outage: **1,200 / 1,200 events delivered**, backlog drained in 21 s |
| AWS CDK blueprint (MSK Serverless, 5× RDS, ElastiCache, EKS + IRSA, SQS + DLQ, WAF, ECR, AMP) | `infra/aws-blueprint` | `cdk synth`: 78 resources; 12/12 assertion checks incl. least-privilege bridge role |
| Helm chart (kind or EKS) | `deploy/helm/skybook` | templates rendered and validated for the kind and EKS profiles |

## Testing

- **Unit tests (31)**: idempotency branches with a mocked repository, request hashing, the saga state machine, retry routing,
  the mock payment provider, JWT auth, search validation, and the bridge (contracts, partial-failure retry, outage → no commit).
- **Integration tests (9)**: real PostgreSQL. Uses `TEST_DATABASE_URL` if set, otherwise Testcontainers. Covers the idempotency race,
  replay and 422, overbooking with both lock strategies, duplicate events, compensation, and hold expiry.
- **E2E (23 checks)**: `scripts/e2e.ts` runs through the gateway against the running stack.
- **Load**: k6 scripts in `load-tests/`.

## Project layout

```
packages/shared/      db, kafka, outbox, idempotent consumer, events (zod), http app, metrics, logger
services/<name>/src/  gateway · flight-search · booking · inventory · payment · notification · aws-bridge
scripts/              seed-flights, e2e, dlq-replay, explain-benchmark, fake-sqs, bridge-outage-test
tests/integration/    PostgreSQL-backed tests
load-tests/           k6 scripts
deploy/               postgres init, prometheus (+alerts), grafana provisioning + dashboard, helm/skybook chart
infra/aws-blueprint/  AWS CDK (TypeScript) production blueprint, synth-only, with assertion checks
infra/iam/            least-privilege IAM policies for the bridge and the outage-test verifier
docs/                 architecture, benchmarks, failure tests, AWS setup, AWS cost estimate, Kubernetes, progress
```

## Status

See [docs/progress.md](docs/progress.md) for what's done, what's verified, and what's still to run on real infrastructure
(the real-AWS bridge test, a kind install, k6 numbers).
