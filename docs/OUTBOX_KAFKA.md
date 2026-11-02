# Transactional Outbox → Kafka → Ledger: Runbook

Delivery is **at-least-once**. Exactly-once *effect* comes from consumer-side idempotency:
`ledger_entry.event_id` is the primary key and the insert is `ON CONFLICT DO NOTHING`.
Ordering across retries, lease reclaims and rebalances is **not** guaranteed and not needed.

```
POST /api/orders ──► Mongo txn { orders, outboxes (PENDING) }
                                   │
                         outbox relay (lease + fencing token)
                                   ▼
                       Kafka  order.placed (3 partitions, key = userId)
                                   ▼
                  ledger-service (Spring Boot) ──► Postgres ledger_entry / ledger_item
                       │ transient failure                │ invalid event
                       ▼                                  ▼
           order.placed-retry-0..4                 order.placed-dlt
```

## 1. Start the stack

```bash
docker compose -f docker-compose.infra.yml up -d
docker compose -f docker-compose.infra.yml --profile app up -d --build ledger-service
```

| Component | Address (host) |
|---|---|
| Mongo (replica set rs0) | `mongodb://127.0.0.1:27017/shopify?directConnection=true` |
| Kafka (external listener) | `localhost:9092` (in-network: `kafka:29092`) |
| Postgres | `localhost:5432`, db `ledger_db` |
| Jaeger UI / OTLP HTTP | `http://localhost:16686` / `http://localhost:4318` |
| Ledger API | `http://127.0.0.1:8090` (loopback only, **no auth in Phase 1**) |
| Ledger actuator | container port `8081` (not published) |
| Relay health | `http://localhost:8085/healthz` |

Node processes (host), with tracing:

```bash
pnpm run backend:otel     # API (nodemon)   or: pnpm run start:otel
pnpm run relay:otel       # outbox relay
```

Without tracing use `pnpm run backend` / `pnpm run relay`.

## 2. Configuration

Relay (all optional):

| Variable | Default | Notes |
|---|---|---|
| `MONGO_URI` | none, required | |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated |
| `OUTBOX_LEASE_MS` | `30000` | claim lease |
| `OUTBOX_SEND_TIMEOUT_MS` | `8000` | startup fails unless `sendTimeout * 3 <= lease` |
| `OUTBOX_BATCH_SIZE` | `100` | rows drained per cycle |
| `OUTBOX_IDLE_SLEEP_MS` | `300` | sleep only when the queue is empty |
| `OUTBOX_MAX_ATTEMPTS` | `5` | only unknown errors count; broker outages never do |
| `OUTBOX_HEALTH_PORT` | `8085` | `/healthz`, `/livez` |

Tracing:

| Variable | Default | Notes |
|---|---|---|
| `OTEL_SDK_DISABLED` | unset | `true` turns Node tracing off |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | Java container uses `http://jaeger:4318` (set in compose) |
| `OTEL_SERVICE_NAME` | `shopify-backend` / `outbox-relay` | chosen from the entry file; Java = `ledger-service` |
| `OTEL_LOG_LEVEL` | unset | `debug` prints SDK diagnostics (Node) |
| `LEDGER_OTEL_ENABLED` | `true` | compose switch for the Java agent |

Metrics and logs exporters are `none` on both sides because Jaeger only ingests traces.

## 3. Verify tracing end to end

Requirements: Node ≥ 18.19 (20.6+ recommended); the ESM loader hook is registered by `backend/otel.js`.

1. Start the stack and both Node processes with the `:otel` scripts. Each prints `[otel] tracing enabled service=… endpoint=…`.
2. Place one order through the API (cookie JWT, items as `{ _id, qty }`).
3. Open `http://localhost:16686`. Services `shopify-backend`, `outbox-relay` and `ledger-service` should all appear.
4. Open the `shopify-backend` trace for `POST /api/orders`. It should contain, in one trace:
   - the HTTP server span and Express spans,
   - MongoDB spans (the order and outbox write inside the transaction),
   - the relay's `outbox publish order.placed` span and the kafkajs send span,
   - the ledger-service Kafka consume span and the JDBC insert.
5. If a service is missing, rerun it with `OTEL_LOG_LEVEL=debug` (Node) or check `docker compose -f docker-compose.infra.yml logs ledger-service | grep -i otel`.

Notes:
- The relay restores the trace saved in `outboxes.traceCarrier`, so the publish span is a child of the original request even though it runs seconds later.
- Mongo spans exist only under an existing span (`requireParentSpan`). The relay's claim/mark queries are intentionally not traced.
- Spans from the final `markPublished` write are not traced either, for the same reason.

## 4. Operations

Outbox state (admin cookie required):

```bash
# GET /api/queues/outbox/stats
# POST /api/queues/outbox/requeue-failed   body (optional): { "eventIds": ["<uuid>"] }
```

Direct inspection:

```bash
docker compose -f docker-compose.infra.yml exec mongo mongosh --quiet shopify --eval "db.outboxes.aggregate([{ \$group: { _id: '\$status', n: { \$sum: 1 } } }]).toArray()"
docker compose -f docker-compose.infra.yml exec postgres psql -U ledger -d ledger_db -c "SELECT COUNT(*) FROM ledger_entry"
docker compose -f docker-compose.infra.yml exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic order.placed-dlt
```

A `[ALERT] … FAILED` log line from the relay means a row hit a permanent error or exhausted its attempts. Fix the cause, then requeue.

Ledger metrics (inside the container, port 8081): `ledger_events_ingested_total`, `ledger_events_duplicate_total`, `ledger_events_invalid_total`.

```bash
docker compose -f docker-compose.infra.yml exec ledger-service curl -s localhost:8081/actuator/prometheus | grep ledger_events
```

Replaying consumer offsets (the consumer group must be inactive, so stop `ledger-service` first). `--shift-by` applies **per partition**:

```bash
docker compose -f docker-compose.infra.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --group ledger-service --topic order.placed \
  --reset-offsets --shift-by -10 --execute
```

Dead-letter topic: `order.placed-dlt` holds invalid events byte for byte (no retries) and valid events that outlasted the retry window (about 60 s of backoff across `order.placed-retry-0..4`). Redrive valid DLT events back to `order.placed` with the Part 1 redrive tool; duplicates are harmless because ingestion is idempotent.

## 5. Failure drills and pass criteria

Run them with the Part 1 drill runner; its header documents usage.

| # | Drill | Pass criteria |
|---|---|---|
| 1 | Volume (100 orders) | k6 201 count = Mongo `PUBLISHED` rows = `ledger_entry` rows |
| 2 | Crash recovery (`docker kill` ledger mid-run) | Rebalance completes, `ledger_entry` ends exactly at the order count, 0 DLT messages |
| 3 | Duplicate replay (stop consumer, shift offsets, restart) | duplicates ignored = number actually replayed, `ledger_entry` count unchanged |
| 4 | Poison pills (invalid bytes, valid-JSON-invalid-schema) | Both land in `order.placed-dlt` without retries; later valid orders still process |
| 5 | Broker outage (`docker stop kafka`, 20 orders) | HTTP 201 throughout; outbox drains to `PUBLISHED` after restart; 0 `FAILED` rows |
| 6 | Two relays | 100 distinct event IDs, no database errors |
| 7 | Relay `kill -9` between send and mark | Duplicate in Kafka, still one ledger row |
| 8 | Postgres outage (~20 s) | Documented DLT outcome; redrive restores the ledger |

## 6. Known limits (Phase 1)

- The ledger API has no authentication. It binds to `127.0.0.1` only; JWT validation is Phase 2.
- KafkaJS idempotence is experimental and per-producer-session, so relay restarts can duplicate. The consumer absorbs that.
- Retry topics are pre-created (`retry-0..4`). If `ledger.retry.attempts` changes, update the topic list in `kafka-init`.