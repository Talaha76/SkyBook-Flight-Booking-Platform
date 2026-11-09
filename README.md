<div align="center">

# SkyBook — Flight Booking Microservices

**Event-Driven Flight Search & Booking Platform with Kafka Sagas, Idempotent APIs, and an AWS SQS Bridge**

[![Node.js](https://img.shields.io/badge/Node.js-22-339933?logo=nodedotjs&logoColor=white)](https://nodejs.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Java](https://img.shields.io/badge/Java-21-ED8B00?logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-3-6DB33F?logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-Cache-DC382D?logo=redis&logoColor=white)](https://redis.io/)
[![AWS SQS](https://img.shields.io/badge/AWS-SQS_%7C_CDK-FF9900?logo=amazonaws&logoColor=white)](https://aws.amazon.com/sqs/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Helm-326CE5?logo=kubernetes&logoColor=white)](https://kubernetes.io/)
[![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboards-F46800?logo=grafana&logoColor=white)](https://grafana.com/)

**Run locally:** `docker compose up --build -d` → API + Swagger at `http://localhost:8080/docs`

</div>


## Table of Contents

- [Measured Results](#measured-results)
- [Screenshots](#screenshots)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [Booking Saga](#booking-saga)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Local Development](#local-development)
  - [1. Clone the Repository](#1-clone-the-repository)
  - [2. Start the Stack](#2-start-the-stack)
  - [3. Verify Everything Works](#3-verify-everything-works)
- [Try the API](#try-the-api)
- [Idempotent Booking Flow](#idempotent-booking-flow)
- [Search & Caching Flow](#search--caching-flow)
- [Reliability Patterns](#reliability-patterns)
- [Kafka → AWS SQS Bridge](#kafka--aws-sqs-bridge)
- [Inventory in Java / Spring Boot](#inventory-in-java--spring-boot)
- [Kubernetes Deployment](#kubernetes-deployment)
- [AWS Production Blueprint](#aws-production-blueprint)
- [API Overview](#api-overview)
- [Monitoring](#monitoring)
- [Benchmarks](#benchmarks)
- [Testing](#testing)
- [Configuration](#configuration)
- [Design Notes](#design-notes)
- [Limitations](#limitations)
- [Future Improvements](#future-improvements)
- [Project Goal](#project-goal)

---

SkyBook is a flight-booking back end built as **Node.js/TypeScript microservices** that communicate through **Apache Kafka**, with the Inventory service also implemented in **Java 21 / Spring Boot** as a drop-in replacement. Every service owns its own **PostgreSQL** database, flight search is cached in **Redis**, and bookings run through a **choreographed saga** that holds a seat, charges a (mock) payment, and automatically **releases the seat when payment fails**.

The project focuses on the problems real booking systems have to solve: duplicate requests, overbooking, lost events, partial failures, traffic spikes, and integrating on-premises event streams with **AWS-native services**.

> **Portfolio project:** SkyBook demonstrates microservice architecture, idempotent REST APIs, the transactional outbox pattern, sagas with compensation, duplicate-safe consumers, query tuning, cache invalidation, retry/dead-letter queues, a Kafka → Amazon SQS bridge, a polyglot (Node.js + Java/Spring Boot) service swap, Kubernetes deployment, and Prometheus/Grafana monitoring — with **every number below measured, not estimated**.

## Measured Results

All results were produced by the project's own test and benchmark scripts (`scripts/run-benchmarks.ps1`, the bridge outage test, and the e2e suite) on a Windows + Docker Desktop machine (8 vCPU), with the bridge running against **real Amazon SQS**. Full details: [`docs/results-2026-11-07.md`](docs/results-2026-11-07.md).

| What was measured | Result |
|---|---|
| Flight search on **1,000,000** flights (k6, p95) | **1,021 ms → 28 ms** with a composite index → **25.5 ms** with Redis (**89% hit rate**, 1,641 req/s) |
| `EXPLAIN ANALYZE` (single query, p95) | **73.3 ms → 0.28 ms** (sequential scan → index scan) |
| 100 identical concurrent booking requests (one `Idempotency-Key`) | **1 booking, 0 duplicates** |
| 200 concurrent seat holds on a 150-seat flight | exactly **150 held / 50 rejected** (pessimistic and optimistic locking) |
| **10× booking spike** (5/s → 50/s) | 3,575 bookings, 17,875 events, **0 lost messages**, 0% errors, all consumers caught up in 161 s |
| Kafka → **real Amazon SQS** through a **2-minute network outage** | **1,200 / 1,200 events delivered**, backlog drained in 21 s |
| Kubernetes (kind + Helm) | 13 pods running, **23/23 end-to-end checks passed** |
| Inventory swapped to **Java 21 / Spring Boot** (no other service changed) | **23/23 end-to-end checks passed**; 200 holds on 150 seats → **150 / 50** for both lock strategies (618 ms pessimistic, 479 ms optimistic) |

---

## Key Features

- **Idempotent booking API** — `POST /bookings` requires an `Idempotency-Key`; retries return the saved response, concurrent duplicates get `409`, reused keys with a different body get `422`.
- **Transactional outbox** in every producing service — no "dual write" between the database and Kafka.
- **Choreographed saga with compensation** — seat hold → payment → confirm, or release the seat and cancel.
- **Duplicate-safe consumers** — every event is applied exactly once per consumer via a `processed_events` table.
- **Overbooking prevention** — pessimistic (`SELECT … FOR UPDATE`) and optimistic (versioned update) locking, both tested.
- **Seat-hold expiry sweeper** — a crashed payment never strands a seat.
- **Query tuning** — composite index verified with `EXPLAIN ANALYZE` on a 1M-row table.
- **Redis cache-aside** with stampede protection (`SET NX PX`) and **event-driven invalidation**.
- **Retry topics + dead-letter queue** (1m → 5m → 30m → DLQ) with a **replay tool**.
- **Kafka → AWS SQS bridge** that commits offsets only after AWS confirms and pauses during outages.
- **JWT authentication** with `CUSTOMER` / `AGENT` roles, Redis rate limiting, and request IDs traced end to end.
- **Prometheus metrics + Grafana dashboard + alert rules** for latency, consumer lag, outbox backlog, and DLQ growth.
- **Two Inventory implementations** — Node.js/TypeScript and **Java 21 / Spring Boot**, interchangeable with one Compose override or one Helm value.
- **Docker Compose** for local development, **Helm chart** for Kubernetes, **AWS CDK blueprint** for production.

---

## System Architecture

```text
                         ┌─────────────────────────────────────┐
  client / k6 ─────────▶ │            API Gateway :8080        │  JWT · roles · rate limit
                         │   X-Request-Id · Swagger UI (/docs) │  (Redis)
                         └──────────────────┬──────────────────┘
          ┌──────────────┬──────────────────┼──────────────┬────────────────┐
          ▼              ▼                  ▼              ▼                ▼
   ┌─────────────┐ ┌────────────┐   ┌─────────────┐ ┌────────────┐ ┌──────────────┐
   │Flight Search│ │  Booking   │   │  Inventory  │ │  Payment   │ │ Notification │
   │ Postgres +  │ │ Postgres:  │   │ Postgres:   │ │  (mock)    │ │ retries +    │
   │ Redis cache │ │ idempotency│   │ seat holds  │ │ Postgres   │ │ DLQ + replay │
   │ read model  │ │ + outbox   │   │ + outbox    │ │ + outbox   │ │ + outbox     │
   └──────▲──────┘ └─────┬──────┘   └──────┬──────┘ └─────┬──────┘ └──────┬───────┘
          │              │ outbox          │ outbox       │ outbox        │
          │              ▼                 ▼              ▼               ▼
          │   ┌───────────────────────────────────────────────────────────────────┐
          └───│                        Apache Kafka (KRaft)                       │
   cache      │ booking.events · inventory.events · payment.events               │
 invalidation │ notification.retry.{1m,5m,30m} · notification.dlq                │
              │                (all keyed by flightId → per-flight ordering)      │
              └──────────────────────────────┬────────────────────────────────────┘
                                             │
                                             ▼
                              ┌──────────────────────────────┐
                              │  AWS Bridge (KafkaJS + SDK)  │ commit after AWS ack,
                              │  SendMessageBatch (10/call)  │ pause on outage
                              └──────────────┬───────────────┘
                                             ▼
                              ┌──────────────────────────────┐
                              │  Amazon SQS flight-events    │
                              └──────────────────────────────┘

        Prometheus scrapes /metrics on every service ──▶ Grafana dashboard + alerts
```

The Inventory box can be either the Node.js service or the Java/Spring Boot one; both use the same topics, consumer group, tables, and HTTP API.

**Rule:** no service reads another service's database. Services communicate only through Kafka events; external clients go through the gateway.

| Service | Responsibility | Owns |
|---|---|---|
| **Gateway** | JWT auth, roles, rate limiting, request IDs, routing, OpenAPI | – |
| **Flight Search** | Search by route and date (read-heavy) | `flights`, `airports`, Redis cache |
| **Booking** | Idempotent booking API, saga state machine | `bookings`, `idempotency_keys`, `outbox_events` |
| **Inventory** (Node.js or Java) | Seat holds, overbooking prevention, hold expiry | `seat_inventory`, `seat_holds` |
| **Payment** | Mock provider with configurable delay and failure rate | `payments` |
| **Notification** | Confirmation/cancellation messages, retries, DLQ | `notifications` |
| **AWS Bridge** | Forwards booking events to Amazon SQS without loss | Kafka offsets (checkpoint) |

---

## Booking Saga

```text
POST /bookings ──▶ BookingRequested ──▶ Inventory: hold seat
                                            │
                    ┌───────────────────────┴───────────────────────┐
                    ▼                                               ▼
                SeatHeld ──▶ Payment: charge                 SeatHoldFailed
                                 │                                  │
               ┌─────────────────┴──────────────┐                   ▼
               ▼                                ▼            Booking: REJECTED
       PaymentSucceeded                   PaymentFailed
               │                                │
               ▼                                ▼
     Booking: CONFIRMED            Inventory: release seat  (compensation)
               │                                │
               ▼                                ▼
         Notification               Booking: CANCELLED ──▶ Notification
```

**Booking states:** `PENDING → SEAT_HELD → CONFIRMED`, or `REJECTED`, or `CANCELLED`.

Seat holds expire after 10 minutes; a sweeper releases expired holds and cancels the booking, so a crashed payment service can never strand a seat.

---

## Technology Stack

### Services

- Node.js 22
- TypeScript
- Express 5
- Zod (request and event validation)
- KafkaJS
- node-postgres (`pg`)
- ioredis
- pino (structured JSON logs)
- prom-client (Prometheus metrics)
- jsonwebtoken (JWT)

### Inventory (Java implementation)

- Java 21
- Spring Boot 3.2 (Web, JDBC, Actuator)
- Spring Kafka
- JdbcTemplate + Flyway
- Micrometer (Prometheus)
- Logback + Logstash encoder (JSON logs)
- Maven

### Data & Messaging

- Apache Kafka 3.8 (KRaft mode, no ZooKeeper)
- PostgreSQL 16 (one database per service)
- Redis 7

### Cloud & Infrastructure

- AWS SDK v3 (Amazon SQS)
- AWS CDK (TypeScript) — production blueprint
- Docker & Docker Compose
- Kubernetes (kind) & Helm
- Prometheus & Grafana

### Testing

- Jest & ts-jest
- JUnit 5, Mockito & Testcontainers (Java)
- PostgreSQL integration tests (Testcontainers supported)
- k6 load tests
- End-to-end test suite through the gateway
- GitHub Actions CI

---

## Repository Structure

```text
SkyBook-Flight-Booking-Platform/
├── packages/
│   └── shared/              # db, kafka, outbox, idempotent consumer, events, http, metrics, logger
│
├── services/
│   ├── gateway/             # JWT, rate limit, routing, openapi.yaml
│   ├── flight-search/       # search, Redis cache, read model
│   ├── booking/             # idempotency, saga state machine
│   ├── inventory/           # seat holds, locking, expiry sweeper
│   ├── inventory-java/      # same service in Java 21 / Spring Boot (Maven)
│   ├── payment/             # mock payment provider
│   ├── notification/        # retry topics, DLQ
│   └── aws-bridge/          # Kafka → Amazon SQS
│
├── scripts/                 # seed, e2e, DLQ replay, benchmarks, outage test, fake SQS
├── tests/integration/       # PostgreSQL-backed tests
├── load-tests/              # k6: search, booking burst, idempotency race
│
├── deploy/
│   ├── helm/skybook/        # Kubernetes Helm chart
│   ├── prometheus/          # scrape config + alert rules
│   ├── grafana/             # provisioned dashboard
│   └── postgres/            # database-per-service init
│
├── infra/
│   ├── aws-blueprint/       # AWS CDK: MSK, RDS, ElastiCache, EKS, SQS, WAF
│   └── iam/                 # least-privilege IAM policies
│
├── docs/                    # architecture, results, benchmarks, failure tests, AWS, Kubernetes
├── docker-compose.yml
├── docker-compose.aws.yml   # real-AWS override for the bridge
├── docker-compose.java.yml  # swap in the Java Inventory service
├── Dockerfile
└── README.md
```

---

## Local Development

### Prerequisites

Install:

- Docker Desktop (about 6 GB of memory available)
- Git

Node.js is **not** required — every service, Kafka, PostgreSQL, and Redis run in containers.

---

## 1. Clone the Repository

```bash
git clone https://github.com/Talaha76/SkyBook-Flight-Booking-Platform.git
cd SkyBook-Flight-Booking-Platform
```

---

## 2. Start the Stack

```bash
docker compose up --build -d
docker compose ps
```

Wait until `gateway` shows **(healthy)**. A one-time `seed` job loads 20,000 flights automatically.

| Open | URL |
|---|---|
| Swagger UI | http://localhost:8080/docs |
| Grafana dashboard | http://localhost:3000 |
| Prometheus | http://localhost:9090 |

---

## 3. Verify Everything Works

```bash
# End-to-end: search, auth, idempotency, saga, compensation, notifications (23 checks)
docker compose --profile tools run --rm e2e

# Unit + integration tests against real PostgreSQL
docker compose --profile tools run --rm tests
```

Expected: `ALL CHECKS PASSED` and `Tests: 40 passed`.

Stop with `docker compose down` (add `-v` to delete the database volume).

---

## Try the API

Demo users (password `skybook`): `alice`, `bob` (CUSTOMER), `agent` (AGENT).

```powershell
$login = Invoke-RestMethod -Method Post -Uri http://localhost:8080/auth/login -ContentType 'application/json' -Body '{"username":"alice","password":"skybook"}'
$h = @{ Authorization = "Bearer $($login.accessToken)" }
$date = (Get-Date).AddDays(2).ToString('yyyy-MM-dd')
$flights = Invoke-RestMethod "http://localhost:8080/flights/search?origin=YYZ&destination=YVR&date=$date"

$body = @{ flightId = $flights.flights[0].id; passengerName = 'Alice Martin'; passengerEmail = 'alice@example.com'; paymentToken = 'tok_success' } | ConvertTo-Json
$booking = Invoke-RestMethod -Method Post -Uri http://localhost:8080/bookings -Headers ($h + @{ 'Idempotency-Key' = [guid]::NewGuid().ToString() }) -ContentType 'application/json' -Body $body
Start-Sleep 2
Invoke-RestMethod "http://localhost:8080/bookings/$($booking.id)" -Headers $h    # status: CONFIRMED
```

Payment tokens: `tok_success` always succeeds, `tok_fail` is always declined (watch the seat get released), anything else fails at a configurable random rate.

---

## Idempotent Booking Flow

```text
POST /bookings + Idempotency-Key
    ↓
Hash request (method + path + user + body, SHA-256)
    ↓
INSERT idempotency key … ON CONFLICT DO NOTHING   ← the unique constraint is the lock
    ↓
New key?
 ├─ yes → ONE transaction: booking row + outbox event + saved response → 201
 └─ no  → same hash + COMPLETED   → replay saved response (201)
          same hash + IN_PROGRESS → 409 Conflict + Retry-After
          different hash          → 422 (key reused with a different request)
```

Verified: **100 identical concurrent requests → 1 booking, 0 duplicates.**

---

## Search & Caching Flow

```text
GET /flights/search?origin&destination&date
    ↓
Redis GET search:{origin}:{dest}:{date}
 ├─ hit  → return cached result
 └─ miss → SET lock NX PX 2000 (one request rebuilds; others wait briefly)
             ↓
          PostgreSQL (composite index on origin, destination, date, time)
             ↓
          cache result for 60 s
```

Seat availability changes arrive as `inventory.events`; Flight Search updates its read model and **deletes the matching cache key after the database commit**, so searches never show stale availability for long.

---

## Reliability Patterns

| Pattern | Where | What it prevents |
|---|---|---|
| Transactional outbox | Booking, Inventory, Payment, Notification | Database commit succeeds but the Kafka publish is lost (or the reverse) |
| `processed_events` deduplication | Every consumer | At-least-once delivery applying an event twice |
| Offsets committed only after the DB commit | Shared consumer | Losing an event when a service crashes mid-processing |
| Pessimistic / optimistic seat locking | Inventory | Overbooking under concurrent requests |
| Hold expiry sweeper | Inventory | Seats stranded by a crashed payment service |
| Retry topics + DLQ + replay tool | Notification | Transient failures becoming lost messages |
| Circuit breaker (pause/resume) | AWS Bridge | Losing events during an AWS outage |
| Fail-open rate limiter, cache fallback | Gateway, Flight Search | A Redis outage taking down the API |

Failure tests (Payment killed mid-booking, Kafka stopped, AWS unreachable, Redis down) are documented in [`docs/failure-tests.md`](docs/failure-tests.md).

---

## Kafka → AWS SQS Bridge

The bridge consumes `booking.events` and `inventory.events` and forwards each event to **Amazon SQS** as a versioned contract (`skybook.booking.confirmed.v1`, `skybook.seat.held.v1`, …).

- Sends with `SendMessageBatch` (up to 10 messages per call).
- **Commits Kafka offsets only after AWS confirms** each batch.
- Retries only the entries AWS returns in `Failed[]`, with exponential backoff.
- On repeated failures, **pauses its Kafka partitions** and lets Kafka hold the backlog, then resumes all partitions on the first successful send.
- Copies the Kafka `eventId` into an SQS message attribute for downstream deduplication.
- Strips sensitive fields (payment tokens) before anything leaves Kafka.
- Uses short-lived `aws login` credentials locally and IRSA on EKS — **no access keys in code**.

**Measured against real Amazon SQS:** a 2-minute network outage during 300 bookings → **1,200 / 1,200 events delivered**, none lost, backlog drained in 21 s.

```bash
# Local, no AWS account (fake SQS):
docker compose --profile aws up -d
docker compose --profile aws --profile tools run --rm bridge-outage-test
```

Real-AWS setup: [`docs/aws-setup.md`](docs/aws-setup.md).

---

## Inventory in Java / Spring Boot

`services/inventory-java` is a second implementation of the Inventory service in **Java 21 and Spring Boot 3**. It is a **drop-in replacement**: same port, environment variables, tables, Kafka topics and consumer group, event envelope, HTTP API, and Prometheus metric names, so no other service changes when it is swapped in.

| Concern | Node.js version | Java / Spring Boot version |
|---|---|---|
| Overbooking prevention | `SELECT … FOR UPDATE` / versioned `UPDATE` | the same SQL through `JdbcTemplate` |
| Dedupe + seat change + outbox row in one transaction | `withTx` | `@Transactional` service in its own bean (called through the Spring proxy) |
| Offsets committed only after the DB commit | KafkaJS manual commit | Spring Kafka `MANUAL_IMMEDIATE` ack after the transaction returns |
| Retry without skipping records | consumer retry loop | `DefaultErrorHandler` with exponential backoff |
| Outbox publisher and hold sweeper | timers | `@Scheduled` beans (`FOR UPDATE SKIP LOCKED`) |
| Schema | migrations on start-up | Flyway with `baseline-on-migrate` on the existing tables |
| Metrics | prom-client | Micrometer, exported under the same names (+ JVM metrics) |

```bash
docker compose -f docker-compose.yml -f docker-compose.java.yml up --build -d                         # swap in Java
docker compose -f docker-compose.yml -f docker-compose.java.yml --profile tools run --rm e2e          # 23 checks
docker compose -f docker-compose.yml -f docker-compose.java.yml --profile tools run --rm tests-java   # mvn verify
docker compose up -d --force-recreate inventory                                                       # back to Node
```

**Verified:** the unchanged e2e suite passed **23/23** with the Java service; `mvn verify` passed **21 unit + 8 integration tests** against real PostgreSQL and Kafka, including 200 concurrent holds on a 150-seat flight (**150 held / 50 rejected**, both strategies) and a Kafka round trip where a redelivered event is detected as a duplicate. On Kubernetes, the implementation is a Helm value: `--set inventory.implementation=java`. Details: [`docs/java-inventory.md`](docs/java-inventory.md).

---

## Kubernetes Deployment

The Helm chart in `deploy/helm/skybook` deploys every service (with readiness/liveness probes, non-root containers, Prometheus annotations, a NetworkPolicy, and a seed Job). With default values it also runs dev-only Kafka, PostgreSQL, and Redis inside the cluster.

```bash
kind create cluster --name skybook
kind load docker-image skybook-app:latest --name skybook
helm install skybook ./deploy/helm/skybook --namespace skybook --create-namespace
kubectl -n skybook get pods
```

Verified on kind: **13 pods running, 23/23 end-to-end checks passed** against the cluster. `inventory.implementation: node | java` selects the Inventory image (the Java pod gets a startup probe and a larger memory limit). Details: [`docs/kubernetes.md`](docs/kubernetes.md).

---

## AWS Production Blueprint

`infra/aws-blueprint` is an **AWS CDK (TypeScript)** design of the production deployment — synthesized and checked, **not deployed**:

| Local component | AWS production equivalent |
|---|---|
| Kafka (KRaft container) | Amazon MSK Serverless (IAM auth) |
| PostgreSQL per service | Amazon RDS for PostgreSQL (one instance per service) |
| Redis | Amazon ElastiCache (Multi-AZ) |
| kind + Helm | Amazon EKS + the same Helm chart, IRSA for pod credentials |
| Container image | Amazon ECR |
| Secrets in env vars | AWS Secrets Manager |
| Gateway exposure | Application Load Balancer + AWS WAF |
| Prometheus | Amazon Managed Service for Prometheus |

`cdk synth` produces 78 CloudFormation resources and passes 12 assertion checks, including a bridge role limited to `sqs:SendMessage` on a single queue. A cost breakdown (≈ $923/month as designed, ≈ $420 with MSK Provisioned) is in [`docs/aws-cost-estimate.md`](docs/aws-cost-estimate.md).

---

## API Overview

All endpoints are served through the gateway on port `8080`:

```text
POST   /auth/login                  # demo JWT login

GET    /flights/search              # ?origin=YYZ&destination=YVR&date=YYYY-MM-DD
GET    /flights/{id}
GET    /airports

POST   /bookings                    # requires Bearer token + Idempotency-Key
GET    /bookings                    # customers: own bookings; agents: all
GET    /bookings/{id}

GET    /inventory/{flightId}        # AGENT only
GET    /payments/{bookingId}        # AGENT only
GET    /notifications               # AGENT only

GET    /health
GET    /ready
GET    /metrics
GET    /docs                        # Swagger UI
GET    /openapi.json
```

---

## Monitoring

Every service exposes `/metrics` for Prometheus:

- `http_request_duration_seconds` (latency histogram per route and status)
- `kafka_consumer_lag`
- `outbox_pending_total`
- `cache_hits_total` / `cache_misses_total`
- `dlq_messages_total`
- `bridge_forwarded_total`, `bridge_paused`, `bridge_end_to_end_seconds`

The provisioned **Grafana dashboard** shows request rate, p95 latency, 5xx errors, cache hit rate, consumer lag, outbox backlog, consumed events, notifications/DLQ, bridge throughput, and Inventory heap usage (JVM or Node).

**Alert rules:** DLQ growth, outbox backlog, high consumer lag, bridge paused, service down.

Structured JSON logs carry `requestId`, `bookingId`, and `eventId`; the `X-Request-Id` travels from HTTP into Kafka headers, so a single booking can be followed across all services.

---

## Benchmarks

One command runs every measurement and writes `load-tests/results/RESULTS.md`, including resume-ready summaries:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-benchmarks.ps1
```

| Benchmark | Result |
|---|---|
| Search, no index (k6, 20 VUs) | p95 1,020.7 ms, 35 req/s |
| Search, composite index | p95 28.4 ms, 1,120 req/s |
| Search, index + Redis cache | p95 25.5 ms, 1,641 req/s, 89.1% hit rate |
| Idempotency race (100 concurrent) | 1 stored booking, 0 duplicates |
| 10× spike | 0 lost messages, peak lag 3,092, caught up in 161 s |

The spike test also exposed the system's bottleneck: the **payment consumer** (a deliberate 200 ms mock provider), visible on the Grafana consumer-lag panel. The booking API itself stayed at **22.6 ms p95** because the outbox and Kafka absorb the burst.

---

## Testing

```bash
docker compose --profile tools run --rm tests   # 31 unit + 9 integration tests
docker compose --profile tools run --rm e2e     # 23 end-to-end checks

# Java Inventory: 21 unit + 8 integration tests (JUnit 5, Mockito, real PostgreSQL + Kafka)
docker compose -f docker-compose.yml -f docker-compose.java.yml --profile tools run --rm tests-java
```

- **Unit tests:** idempotency branches (mocked repository), request hashing, the saga state machine, retry routing, the mock payment provider, JWT auth, search validation, and the AWS bridge (partial failures, outages).
- **Integration tests (real PostgreSQL):** 50-request idempotency race, saved-response replay, overbooking with both lock strategies, duplicate event delivery, compensation, hold expiry.
- **End-to-end:** the full saga through the gateway, including payment failure compensation and authorization rules.
- **Java Inventory:** handler outcomes with Mockito; the overbooking test for both strategies, duplicate delivery, compensation, hold expiry, the HTTP contract, and a full Kafka round trip.
- **Load:** k6 scripts in `load-tests/`.

---

## Configuration

All configuration comes from environment variables (see `.env.example`). Key settings:

```env
JWT_SECRET=change-me
INTERNAL_TOKEN=change-me
INVENTORY_LOCK_STRATEGY=pessimistic     # or optimistic
SEAT_HOLD_TTL_SECONDS=600
PAYMENT_FAILURE_RATE=0.1
SEARCH_CACHE_ENABLED=true
NOTIFICATION_RETRY_DELAYS_MS=60000,300000,1800000

# AWS bridge (real AWS)
SQS_QUEUE_URL=https://sqs.<region>.amazonaws.com/<account-id>/skybook-flight-events
AWS_REGION=us-east-2
```

Do not commit `.env` — it is git-ignored. AWS credentials come from your local AWS CLI profile, never from files in this repository.

---

## Design Notes

### Why an outbox instead of publishing directly to Kafka?

Publishing from the request handler creates a dual write: the database commit can succeed while the Kafka publish fails, or the reverse. Writing the event to `outbox_events` in the **same transaction** as the business change makes them atomic; a publisher loop (`FOR UPDATE SKIP LOCKED`) ships rows to Kafka.

### Why at-least-once delivery?

Exactly-once across a database and Kafka is expensive and fragile. SkyBook accepts at-least-once delivery and makes duplicates harmless: every consumer records the `eventId` in the same transaction as its change.

### Why a saga instead of two-phase commit?

Each service owns its data, so there is no shared transaction. The saga uses local transactions plus **compensating actions** (releasing a held seat) to keep the system consistent.

### Why key Kafka messages by `flightId`?

All events for one flight land on the same partition and are processed in order, while different flights are processed in parallel.

### Pessimistic vs. optimistic locking

Both strategies are implemented and pass the 200-request / 150-seat test; the strategy is a configuration switch (`INVENTORY_LOCK_STRATEGY`).

---

## Limitations

SkyBook is a portfolio project, not a production airline system.

- Authentication uses three built-in demo users (HS256 JWTs); production would use an identity provider with RS256/JWKS.
- The payment provider is a mock with configurable delay and failure rate — no real card data is handled.
- Locally, one PostgreSQL server hosts all service databases (schemas remain fully isolated).
- Kubernetes was verified on kind (local), and the AWS blueprint is synthesized but not deployed. The Java Inventory was verified with Docker Compose; its Helm toggle is rendered but not yet installed on kind.
- If a seat hold expires *and* the payment later succeeds, there is no automatic refund step (payment rejects expired holds, which narrows the window).
- Benchmark numbers come from a single developer machine and are not production-scale figures.

---

## Future Improvements

Possible extensions include:

- EventBridge as a second bridge target,
- OpenTelemetry tracing into Jaeger,
- Kafka Schema Registry with Avro/Protobuf event schemas,
- consumer-lag-driven autoscaling (KEDA),
- a k6 throughput comparison of the Node.js and Java Inventory services,
- concurrent payment processing within a partition,
- a refund step for late payments on expired holds,
- contract tests between services (Pact),
- a deployed AWS environment (EKS + MSK + RDS).

---

## Project Goal

SkyBook was built to demonstrate how a booking system can stay **correct under concurrency, duplicates, partial failures, and traffic spikes** — and to prove it with measurements rather than claims.

The project combines:

**Microservices + Event Streaming + Idempotent APIs + Sagas + Caching + AWS Integration + Node.js & Java/Spring Boot + Kubernetes + Monitoring**

into one end-to-end, fully tested flight-booking platform.
