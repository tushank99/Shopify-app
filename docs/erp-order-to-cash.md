# ERP layer: order-to-cash, inventory and purchasing

A Spring Boot + MySQL service (`erp-app`) added next to the existing Kafka -> Postgres `ledger-service`.
It covers the three core flows of an ERP / supply-chain system:

| Flow | What it does |
|------|--------------|
| **Inventory** | Stock per SKU (`on_hand`, `reserved`, `available`), reorder points, reservations per order |
| **Purchasing** | Purchase orders `DRAFT -> APPROVED -> RECEIVED`; receiving adds stock; low-stock items can generate a draft PO |
| **Order-to-cash** | Sales order saga: reserve stock -> post to ledger -> ship, with compensation on failure |

Run it: `docker compose -f docker-compose.infra.yml --profile app up -d --build mysql kafka kafka-init postgres ledger-service erp-service`
then open Swagger UI at <http://localhost:8095/swagger-ui.html> (OpenAPI JSON at `/v3/api-docs`).

## Maven layout

```
commerce-platform (root pom: parent = spring-boot-starter-parent, shared dependencyManagement)
 |- ledger-service   existing Kafka -> Postgres ledger (own Boot parent, own Dockerfile, unchanged)
 |- erp-common       exceptions, OptimisticRetry, ObjectIds
 |- erp-inventory    InventoryItem (@Version), StockReservation, InventoryService, REST
 |- erp-purchasing   PurchaseOrder, goods receipt            (depends on erp-inventory)
 |- erp-orders       SalesOrder, saga, outbox publisher      (depends on erp-inventory)
 `- erp-app          Spring Boot main, Flyway migration, OpenAPI, error handling
```

Domain modules only depend "downwards" (orders/purchasing -> inventory -> common); nothing depends on `erp-app`.

## Design notes

### 1. Preventing overselling (optimistic locking)
`inventory_item.version` is a JPA `@Version`. Reserving stock is `UPDATE ... SET reserved = ?, version = v+1 WHERE sku = ? AND version = v`.
If two transactions read the same row, only one UPDATE matches; the other gets `OptimisticLockingFailureException`.
`OptimisticRetry` then re-runs the **whole transaction** (jittered backoff, 15 attempts in the saga) so it re-reads the winner's committed state and either succeeds or sees "insufficient stock".
Why optimistic, not `SELECT ... FOR UPDATE`: rows are rarely contended, so no lock is held while the app works. The cost is retries on a very hot SKU (see limits).
Backstop: MySQL `CHECK (reserved <= on_hand)` rejects an impossible state even if the code has a bug.

### 2. The saga
Each step is its own local transaction; each writes a row to `saga_step` (append-only audit trail, `GET /api/orders/{id}/saga`).

| Step | Local transaction | Compensation |
|------|-------------------|--------------|
| `RESERVE_STOCK` | reserve every line (all-or-nothing) + order `STOCK_RESERVED` | none needed: it is atomic, a failure leaves nothing behind |
| `POST_LEDGER` | insert `outbox_event` (order.placed v1) + order `LEDGER_POSTED`, same transaction | cancel the event if still `PENDING` |
| `SHIP` | reservations `COMMITTED`, `on_hand` decreases + order `SHIPPED` | release reservations, order `CANCELLED` |

Order status is a small state machine (`OrderStatus.next()`); illegal transitions throw.
To see compensation work: start the service with `ERP_SIMULATE_FAILURE_AT=SHIP` (or `POST_LEDGER`), place an order, then read `/api/orders/{id}/saga`.

### 3. Posting to the ledger without a dual write
The saga never calls Kafka inside the order transaction. It inserts an `outbox_event` row in the same MySQL transaction as the status change;
`OutboxPublisher` polls with `SELECT ... FOR UPDATE SKIP LOCKED` (safe with several instances) and marks rows `PUBLISHED` only after the broker acks.
Delivery is at-least-once; `ledger-service` de-duplicates on `eventId`. The event is the existing `order.placed` v1 contract
(`contracts/order.placed.v1.schema.json`), which is why order, customer and product ids are 24-hex strings. ERP orders therefore appear in the same ledger API as web-shop orders.

### 4. API behaviour
* `Idempotency-Key` header on `POST /api/orders`: a client retry returns the same order (unique index on `sales_order.idempotency_key`).
* Prices are snapshotted on the order lines; later price changes never alter existing orders.
* Errors are RFC 7807 `ProblemDetail`: 400 validation, 404 unknown id, 409 conflict (insufficient stock, wrong status, retries exhausted), 500 saga failure after compensation. Unexpected errors never leak internals.

## Known limits (honest list)
* **Ledger reversal**: if `SHIP` fails after `order.placed` was already published, the ledger needs a reversal event. The v1 contract has none, so the saga records `COMPENSATION_REQUIRED` in `saga_step`. Fix = an `order.cancelled` event (contract v2) consumed by ledger-service.
* **Hot SKU**: thousands of requests on one SKU make optimistic locking retry a lot; some requests get 409 "retry". A flash-sale SKU would move to a Redis counter in front or a pessimistic lock.
* **No authentication** on the ERP API yet (compose binds it to 127.0.0.1 only).
* **Synchronous saga**: the HTTP call waits for all steps. A crash mid-saga leaves a visible status + `saga_step` trail, but there is no recovery job yet that resumes or compensates stuck orders.
