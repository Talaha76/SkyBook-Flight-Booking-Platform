# Failure tests

What was broken, what happened, how the system recovered. Measured in the development sandbox
(services running as Node processes against PostgreSQL 16, Redis 7 and a Kafka-protocol broker).

| # | Failure injected | Observed behaviour | Recovery |
|---|---|---|---|
| 1 | **Payment service killed** before a booking was made | Booking reached `SEAT_HELD` and waited; the seat stayed held (TTL 10 min) | Payment restarted, rejoined its consumer group, read the pending `SeatHeld` event, and the booking became **CONFIRMED 0.9 s after restart** |
| 2 | **Kafka broker stopped**, then a booking was created | `POST /bookings` still returned **201 PENDING**, because the event was committed to `outbox_events` in the same DB transaction | Broker restarted; the outbox publisher flushed the row and the saga completed: **CONFIRMED 7.0 s after the broker came back** |
| 3 | **Payment declined** (`tok_fail`) | `PaymentFailed` → Inventory released the seat (compensation) → booking `CANCELLED` with reason `CARD_DECLINED` | Availability returned to its previous value (e2e check) |
| 4 | **Unknown flight id** | Inventory emitted `SeatHoldFailed(FLIGHT_NOT_FOUND)` → booking `REJECTED` | — |
| 5 | **Email provider permanently failing** (recipient contains `fail-notify`) | Live attempt failed → `notification.retry.1m` → `retry.5m` → `retry.30m` → **`notification.dlq`** after 3 retries; notification row `DEAD`, `dlq_messages_total` incremented | `dlq-replay --since 1h --dry-run` listed the message; without `--dry-run` it is re-published to `retry.1m` |
| 6 | **Duplicate event delivery** | `processed_events` insert conflicts → handler skipped | — |
| 7 | **Hold expiry** (payment never answers) | Sweeper releases holds past `expires_at`, emits `SeatReleased(EXPIRED)` → booking `CANCELLED` | Integration test `expired holds are swept back into inventory` |
| 8 | **Rate limit** exceeded | Gateway returns 429 with `Retry-After`; `gateway_rate_limited_total` increments | Next one-minute window |
| 10 | **AWS (SQS) unavailable for 2 minutes** during 300 bookings | Bridge pauses its partitions (`bridge_paused`=1), Kafka holds up to 978 events, no offsets committed | Circuit closes on the first successful probe and every partition resumes: **1,200/1,200 events in SQS, 0 duplicates, drained in 1.0 s** |
| 9 | **Redis down** | Search falls back to Postgres (`X-Cache: BYPASS`); the rate limiter fails open | Automatic once Redis is reachable |

Tests 1 and 2 were run as scripted experiments in the sandbox. Re-run them on Docker Compose with:

```powershell
# 1. Payment outage
docker compose stop payment
# create a booking (Swagger UI or the e2e script), see it sit at SEAT_HELD, then:
docker compose start payment

# 2. Kafka outage
docker compose stop kafka
# create a booking -> 201 PENDING; check the backlog:
docker compose exec postgres psql -U postgres -d skybook_booking -c "select status, count(*) from outbox_events group by 1"
docker compose start kafka
```

Known limitation: if a hold expires **and** payment later succeeds for that booking (payment slower than
the 10-minute TTL), the booking is cancelled but no refund event is issued. Payment refuses to charge
expired holds (`HOLD_EXPIRED`), which narrows the race, but a refund step would close it fully.
