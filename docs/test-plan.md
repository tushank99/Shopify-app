# Test plan: ERP order-to-cash layer

Scope: `erp-inventory`, `erp-purchasing`, `erp-orders`, `erp-app`. Automated test code is intentionally kept minimal for now;
this plan is the written definition of what must be verified at each level. The **Status** column says what has actually been done.

Legend: **Manual** = executed by hand against the running stack (curl / Swagger UI). **Planned** = not yet automated.

## 1. Unit tests (no Spring, no database) - Planned (JUnit 5 + Mockito)

| ID | Target | Check |
|----|--------|-------|
| U-1 | `InventoryItem.reserve` | rejects qty <= 0; throws `InsufficientStockException` when `available < qty`; increases `reserved` |
| U-2 | `InventoryItem.commitShipment / release` | cannot exceed `reserved`; shipment lowers both `reserved` and `on_hand` |
| U-3 | `OrderStatus` / `SalesOrder.transitionTo` | only the allowed transitions succeed; `SHIPPED` and `CANCELLED` are final |
| U-4 | `PurchaseOrder` | approve only from DRAFT, receive only from APPROVED, cancel not from RECEIVED |
| U-5 | `OptimisticRetry` | retries on `OptimisticLockingFailureException`, gives up after N with `ConcurrentUpdateException`, does not retry other exceptions |
| U-6 | `OrderSagaService` (mocked ports) | failure at SHIP triggers release + cancel; failure in RESERVE_STOCK triggers no compensation |
| U-7 | `LedgerEventFactory` | payload matches `contracts/order.placed.v1.schema.json`; money as 2-decimal strings |

## 2. Integration tests (real MySQL, Kafka) - Planned (Testcontainers)

| ID | Check |
|----|-------|
| I-1 | Flyway migration applies on an empty MySQL 8; `CHECK (reserved <= on_hand)` rejects a bad update |
| I-2 | N threads reserve the last K units: exactly K succeed, `on_hand`/`reserved` consistent, no negative stock |
| I-3 | `POST_LEDGER` writes the order status and the outbox row atomically (force a failure, neither persists) |
| I-4 | `OutboxPublisher` publishes a `PENDING` row and marks it `PUBLISHED`; broker down leaves it `PENDING` and increments `attempts` |
| I-5 | Duplicate `Idempotency-Key` (sequential and concurrent) yields one order |
| I-6 | Receiving a purchase order increases `on_hand` and is not applied twice |

## 3. System tests (whole stack via the REST API) - Manual (done), Planned (automate)

Stack: MySQL + Kafka + Postgres + `ledger-service` + `erp-service` from `docker-compose.infra.yml`.

| ID | Scenario | Expected | Status |
|----|----------|----------|--------|
| S-1 | Create items, place an order of 2 SKUs | `201`, `SHIPPED`, totals correct, stock reduced | Manual: passed |
| S-2 | Repeat the order with the same `Idempotency-Key` | same order id returned, stock reduced once | Manual: passed |
| S-3 | Order more than available | `409`, order `CANCELLED`, nothing reserved | Manual: passed |
| S-4 | Shipped order reaches `ledger-service` | `GET /api/ledger/orders/{id}` returns matching entry | Manual: passed |
| S-5 | 30 parallel orders for 5 units | no overselling (`sold + on_hand = 5`), losers get `409` | Manual: passed (see note) |
| S-6 | `ERP_SIMULATE_FAILURE_AT=SHIP` | `500`, order `CANCELLED`, stock released, saga trail shows compensation | Manual: passed (stock back to 5, ledger event cancelled) |
| S-7 | Low stock -> reorder suggestion -> approve -> receive | PO `RECEIVED`, `on_hand` increased | Planned |
| S-8 | Kafka stopped, then orders placed, then Kafka started | orders succeed; events published after recovery; ledger has all of them | Planned |

Note S-5: the first run (5 retries) lost one SHIP step under 30-way contention on one SKU; it compensated correctly but left 1 unit unsold. With the retry budget at 15 the re-run gave exactly 5 x `201`, 25 x `409`, `on_hand` 0.

## 4. Quality gates
* `mvn -B package` (all modules compile) must pass in CI.
* Coverage target once tests exist: JaCoCo, 70% line coverage on `erp-inventory` and `erp-orders` domain classes.
* Definition of done for a story: acceptance criteria in `docs/user-stories.md` verified, OpenAPI updated, no unhandled 500 in the happy and error paths.
