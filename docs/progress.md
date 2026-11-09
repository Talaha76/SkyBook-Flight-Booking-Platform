# Progress checklist

Legend: ✅ done and verified · 🟡 done, not verified in the dev sandbox · ⬜ not started

## Phase 1: functional core, messaging, reliability, operations

| Item | Status | Verification |
|---|---|---|
| API gateway: JWT, roles, Redis rate limit, X-Request-Id, proxy, Swagger UI | ✅ | e2e (401/403), rate limit observed returning 429, `/docs` served |
| Flight search + composite index + 1M-row seed | ✅ | EXPLAIN ANALYZE before/after (docs/benchmarks.md) |
| Redis cache-aside, stampede lock, invalidation via `inventory.events` | ✅ | e2e `X-Cache: HIT`; read model updated after booking |
| Idempotent `POST /bookings` (replay / 409 / 422 / key release) | ✅ | unit + integration (50-request race → 1 booking) + e2e |
| Transactional outbox (all producing services) | ✅ | Kafka-outage experiment (docs/failure-tests.md) |
| Saga with compensation + hold-expiry sweeper | ✅ | e2e (confirm, cancel, reject) + integration (sweeper) |
| Duplicate-safe consumers | ✅ | integration test |
| Overbooking prevention, pessimistic + optimistic | ✅ | 200/150 integration test for both |
| Mock payment (delay, failure rate, deterministic tokens) | ✅ | unit + e2e |
| Notification retry topics → DLQ → replay tool | ✅ | sandbox run: RETRYING ×3 → DEAD, replay dry run lists it |
| Prometheus metrics, JSON logs, request-id propagation | ✅ | `/metrics` scraped locally; the same requestId seen across services |
| Prometheus alert rules + Grafana dashboard (provisioned) | ✅ | dashboard viewed live on the owner's machine with benchmark traffic |
| Docker Compose stack (Kafka KRaft, Postgres, Redis, services, tools) | ✅ | Owner's Windows machine / Docker Desktop (2026-11-07): `docker compose up --build -d`, all services healthy, **e2e 23/23 passed, tests 32/32 passed** |
| k6 scripts (search, booking burst, idempotency race) | ✅ | run on the owner's machine (docs/results-2026-11-07.md) |
| GitHub Actions CI | 🟡 | written, not run |

How Phase 1 was verified: **on the owner's machine via Docker Compose (official apache/kafka:3.8.0 image)**, and earlier in the dev sandbox: services ran as Node processes against PostgreSQL 16, Redis 7 and a Kafka-protocol
broker (franz-go `kfake`) in the sandbox. KafkaJS, consumer groups, offsets and batching all ran over the real
Kafka wire protocol. Production Compose uses the official `apache/kafka:3.8.0` image.

## Phase 2: advanced infrastructure

| Item | Status | Verification |
|---|---|---|
| Kafka → AWS SQS bridge (KafkaJS + AWS SDK v3; commit-after-ack, `Failed[]` retry, pause/resume circuit breaker, `paymentToken` stripped) | ✅ | 8 unit tests; sandbox run against a fake SQS speaking the AWS JSON protocol with the real AWS SDK: e2e events 15/15, **2-min outage test 1,200/1,200, 0 duplicates, 1.0 s drain**, 30 % `Failed[]` injection → 2,445/2,445 delivered (5 entries retried) |
| Bridge in Docker Compose (`--profile aws`) | ✅ | ran on the owner's Docker Desktop against real SQS |
| Bridge against **real AWS SQS** (`docker-compose.aws.yml`, `aws login` session credentials, us-east-2) | ✅ | owner's machine, 2026-11-07: e2e events forwarded to real SQS |
| Real-AWS outage test (`--manual`, counts by draining the queue) | ✅ | owner's machine: 2-min Wi-Fi outage, **1,200/1,200 delivered**, 1 duplicate, drained in 21.4 s |
| AWS CDK blueprint: MSK Serverless, 5× RDS, ElastiCache Multi-AZ, EKS + node group + IRSA, SQS + DLQ, WAF, ECR, AMP, Secrets Manager | ✅ | `cdk synth` → 78 resources; **12/12 assertion checks** (`npm test`), incl. a bridge role limited to `sqs:SendMessage` on one queue and correct IRSA condition keys (a bug the checks caught and that was fixed) |
| AWS cost estimate | ✅ | docs/aws-cost-estimate.md: ≈ $923/month as written, ≈ $420 with MSK Provisioned; published us-east-1 rates with sources |
| Helm chart (kind + EKS profiles, IRSA, Ingress/WAF, NetworkPolicy, seed Job, dev infra) | ✅ | **deployed on kind on the owner's machine: 13 pods Running, e2e 23/23 passed**; `helm lint` clean. A Kafka readiness bug found in that run was fixed in the chart (docs/kubernetes.md) |
| Grafana dashboard (incl. bridge panels) + alerts | ✅ | dashboard live on the owner's machine, showing benchmark traffic (request rate, p95, cache hit rate) |
| CI job for the blueprint | 🟡 | written, not run |
| Benchmark automation → `RESULTS.md` with filled resume bullets (`scripts/run-benchmarks.ps1` / `.sh`) | ✅ | sandbox: bench-prep (1M reseed, index toggle), EXPLAIN JSON, spike monitor (10× burst of 1,600 bookings: peak lag 1,428, all consumers caught up 74 s after the last request, 0 unconsumed, 1,600/1,600 confirmed), report rendering. k6 itself runs only on your machine |
| Consumer throughput fix found by the spike test: partitions processed in parallel + offsets committed every second | ✅ | the same spike drained in ~130 s instead of more than 300 s |
| k6 measured results on the owner's machine | ✅ | docs/results-2026-11-07.md: search 1,021 → 25.5 ms p95, 89 % hit rate; race 100 → 1 booking; 10× spike 0 lost |
| EventBridge variant, OpenTelemetry → Jaeger (stretch) | ⬜ | out of scope for now |

## Phase 3: Inventory service in Java / Spring Boot

| Item | Status | Verification |
|---|---|---|
| `services/inventory-java`: Spring Boot 3.2, Java 21, JdbcTemplate, Flyway (baseline-on-migrate), Spring Kafka (manual ack after commit, exponential-backoff error handler), `@Scheduled` outbox publisher + hold sweeper, Micrometer with the Node metric names, JSON logs | ✅ | built in Docker on the owner's machine (2026-11-08) |
| Unit tests: 4 classes, 21 tests (Mockito) | ✅ | owner's machine: **21/21 passed** |
| Integration tests: overbooking 200/150 × both strategies, dedupe/compensation/confirm/sweeper/HTTP contract, Kafka round trip | ✅ | owner's machine, real Postgres + Kafka: **8/8 passed**; pessimistic 200 holds / 150 seats in 618 ms → 150 / 50, optimistic in 479 ms → 150 / 50; redelivered BookingRequested logged `result=duplicate` |
| `Dockerfile`, `docker-compose.java.yml` (swap + `tests-java`) | ✅ | owner's machine: all containers healthy with the Java inventory |
| **Drop-in proof: unchanged e2e suite with the Java service** | ✅ | owner's machine: **23/23 checks passed** |
| Helm `inventory.implementation: node \| java` (image, no node command, startup probe, 768 Mi) | 🟡 | rendered for both values; not yet installed on kind |
| CI job `inventory-java` (`mvn verify` + image build) | 🟡 | written, not run |
| Grafana panel: inventory heap (JVM or Node) + GC pause | 🟡 | added to the dashboard JSON; not viewed |
| Optional: k6 booking burst, Node vs Java | ⬜ | not run |

Bug found on the first real run: Spring Boot tests disable metrics export by default, so the `/metrics` controller's
required `PrometheusMeterRegistry` was missing and every integration test failed at start-up. Fixed by making the
registry optional in the controller and enabling metrics in the tests with `@AutoConfigureObservability`.

## Known limitations

- Demo authentication (three built-in users, HS256). A production setup would use an IdP with RS256/JWKS.
- No refund step if payment succeeds after a hold has already expired (Payment rejects expired holds, which narrows the window).
- One PostgreSQL server hosts all service databases (the guide's 8 GB layout); the schemas are still fully isolated.
- The bridge keeps no `bridge_checkpoints` table (the guide lists one): committed Kafka offsets *are* the checkpoint, and downstream dedup uses the `eventId` attribute.
- The services' KafkaJS client uses plaintext/no auth. On MSK Serverless (IAM auth) they would need SASL/OAUTHBEARER with the MSK IAM signer, a small change in `packages/shared/src/kafka.ts`.
