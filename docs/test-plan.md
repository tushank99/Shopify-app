# Test plan: ERP order-to-cash layer

Scope: `erp-common`, `erp-inventory`, `erp-purchasing`, `erp-orders`, `erp-app`.
Run everything: `mvn -B -pl erp-app -am verify` (needs Docker for Testcontainers).
Coverage report: `erp-app/target/site/jacoco-aggregate/index.html` (one report over all ERP modules; CI uploads it and prints a per-module table in the run summary).

| Level | Tools | Tests | Where |
|-------|-------|-------|-------|
| Unit | JUnit 5, Mockito, AssertJ | 109 | `erp-*/src/test` |
| Integration | Spring Boot test, Testcontainers (MySQL 8.4), EmbeddedKafka (KRaft), real Flyway migration | 20 | `erp-app/src/test/.../it` |
| System | the same integration tests drive the real HTTP API end to end; S-8 is manual | - | - |

Result of the last full run: **129 tests, 0 failures. Line coverage 93.9%** (branches 87.7%).

| Module | Line coverage |
|--------|---------------|
| erp-common | 100.0% (23 / 23) |
| erp-inventory | 95.5% (149 / 156) |
| erp-orders | 95.3% (301 / 316) |
| erp-purchasing | 94.3% (99 / 105) |
| erp-app | 75.0% (33 / 44) |
| **total** | **93.9% (605 / 644)** |

## 1. Unit tests (no Spring, no database) - Automated

| ID | Target | Check | Test class |
|----|--------|-------|------------|
| U-1 | `InventoryItem.reserve` | rejects qty <= 0; `InsufficientStockException` when `available < qty`; increases `reserved` | `InventoryItemTest` |
| U-2 | `InventoryItem.commitShipment / release / receive` | cannot exceed `reserved`; shipment lowers `reserved` and `on_hand` | `InventoryItemTest` |
| U-3 | `OrderStatus` / `SalesOrder` | all 25 (from, to) pairs: only the 6 saga transitions succeed; `SHIPPED`, `CANCELLED` are final; price snapshot and totals | `SalesOrderTest` |
| U-4 | `PurchaseOrder` | approve only from DRAFT, receive only from APPROVED, cancel not from RECEIVED | `PurchaseOrderTest` |
| U-5 | `OptimisticRetry` | retries optimistic-lock failures, gives up after N with a 409-mapped exception, never retries other errors | `OptimisticRetryTest` |
| U-6 | `OrderSagaService` (mocked ports) | happy path order of steps; failure in RESERVE_STOCK = no compensation; failure at POST_LEDGER / SHIP = release + cancel event, or `COMPENSATION_REQUIRED` if already published; idempotency (sequential and racing) | `OrderSagaServiceTest` |
| U-7 | `LedgerEventFactory` | payload validates against `contracts/order.placed.v1.schema.json` (the schema ledger-service uses); money as 2-decimal strings | `LedgerEventFactoryTest` |
| U-8 | `InventoryService` | reservation idempotent per order; SKUs locked in sorted order (deadlock avoidance); release/ship only touch the right reservation states | `InventoryServiceTest` |
| U-9 | `PurchaseOrderService` | duplicate SKUs merged; receipt adds every line once; optimistic conflict re-reads and retries | `PurchaseOrderServiceTest` |
| U-10 | `OutboxPublisher` | `PUBLISHED` only after the broker ack; a broker error keeps the row `PENDING`, counts the attempt and stops the batch | `OutboxPublisherTest` |

## 2. Integration tests (real MySQL + Kafka) - Automated

| ID | Check | Test |
|----|-------|------|
| I-1 | Flyway migration applies on an empty MySQL 8; `CHECK (reserved <= on_hand)` and the SKU foreign key reject bad writes | `SchemaIntegrationTest` |
| I-2 | 20 concurrent buyers for 5 units: exactly 5 x 201, the rest 409, `on_hand` 0, no overselling | `ConcurrencyIntegrationTest` |
| I-3 | Failure at SHIP is fully compensated (stock released, event cancelled, saga trail) | `SagaCompensationIntegrationTest` |
| I-4 | Outbox row is published to Kafka, marked `PUBLISHED`; record key, `event-id` header and payload match the stored event | `OutboxKafkaIntegrationTest` |
| I-5 | Duplicate `Idempotency-Key`, sequential and 8 concurrent: one order, stock reduced once | `OrderApiIntegrationTest`, `ConcurrencyIntegrationTest` |
| I-6 | Purchase order receipt adds stock once; second receipt / cancel after receipt -> 409 | `PurchasingIntegrationTest` |

**Database integrity checks.** After *every* integration test, `AbstractIntegrationTest#databaseIsConsistent` runs SQL over the whole database:
stock never negative and `reserved <= on_hand`; `inventory.reserved` equals the sum of open reservations; every `SHIPPED` order has exactly one live ledger event;
at most one event per order; a `CANCELLED` order holds no stock and no pending event; order total = sum of lines; line total = price x qty;
shipped quantity = committed reservation.

## 3. System tests (whole stack via the REST API)

| ID | Scenario | Expected | Status |
|----|----------|----------|--------|
| S-1 | Create items, place an order of 2 SKUs | `201`, `SHIPPED`, totals correct, stock reduced | Automated (`OrderApiIntegrationTest`) |
| S-2 | Repeat the order with the same `Idempotency-Key` | same order id, stock reduced once | Automated |
| S-3 | Order more than available | `409`, order `CANCELLED`, nothing reserved | Automated |
| S-4 | Shipped order reaches `ledger-service` | `GET /api/ledger/orders/{id}` returns the entry | ERP side automated (I-4); ledger side covered by `LedgerIntegrationTest`; full chain manual: passed |
| S-5 | Parallel orders for few units | no overselling | Automated (I-2) |
| S-6 | Failure at SHIP | `500`, order `CANCELLED`, stock released, compensation trail | Automated (I-3) |
| S-7 | Low stock -> reorder suggestion -> approve -> receive | PO `RECEIVED`, `on_hand` increased | Automated (`PurchasingIntegrationTest`) |
| S-8 | Kafka stopped, orders placed, Kafka started | orders succeed; events published after recovery | Manual (planned) |

Also checked: validation errors are `400` ProblemDetails with per-field `errors`; unknown ids are `404 application/problem+json`; `/v3/api-docs` lists every resource; `/actuator/health/readiness` is `UP`.

## 4. Defects found by these tests

| ID | Severity | Found by | Problem | Root cause | Fix |
|----|----------|----------|---------|------------|-----|
| DEF-1 | High | I-2 (20 buyers) | Under concurrent orders, half the requests failed with `500` ("Connection is not available") | The outbox relay claims rows with `SELECT ... WHERE status='PENDING' FOR UPDATE`. Under MySQL REPEATABLE READ that also takes a **gap lock** on the `(status, id)` index, so every saga inserting a new `PENDING` event waited for the relay's transaction (which spans the Kafka sends), holding a pool connection. Confirmed: same test with the relay off had 0 pool timeouts | Relay transaction runs at READ COMMITTED (record locks only, no gap locks). `SKIP LOCKED` still keeps instances apart |
| DEF-2 | Medium | I-2 + U-6 regression test | A non-business failure in `RESERVE_STOCK` (e.g. DB timeout) left the order in `CREATED` forever | Only `ConflictException`/`NotFoundException` cancelled the order | Any failure in that step records `FAILED` and cancels (the step is atomic, nothing to compensate) |
| DEF-3 | Medium | I-2 | Temporary DB unavailability returned a generic `500` | No mapping for transient data-access errors | `503` + `Retry-After: 1` ProblemDetail (nothing was committed, safe to retry) |
| DEF-4 | Medium | `invalidRequestsAre400...` | `POST /api/orders` with an invalid body returned `400` **without** the field errors | The `@Size` on the `Idempotency-Key` header makes Spring validate the whole method and throw `HandlerMethodValidationException`, whose handler dropped the details | Handler now returns every field / header error (`lines[0].quantity`, `Idempotency-Key`, ...) |

## 5. Quality gates
* CI (`.github/workflows/ci.yml`, job `erp-build`) runs `mvn -pl erp-app -am verify` on every push and pull request: all unit + integration tests must pass.
* The JaCoCo report is uploaded as the `erp-test-reports` artifact, with a per-module coverage table in the run summary.
* Definition of done for a story: acceptance criteria in `docs/user-stories.md` covered by a test, OpenAPI updated, no unhandled `500` in the happy and error paths.
