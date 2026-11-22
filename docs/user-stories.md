# User stories -> endpoints

Base URL `http://localhost:8095`. Interactive docs: `/swagger-ui.html`.

### US-1 Sell without overselling
*As a sales clerk, I want an order to reserve stock atomically, so that we never promise goods we do not have.*
**Acceptance**
* `POST /api/orders` with enough stock -> `201`, status `SHIPPED`, `on_hand` reduced.
* Not enough stock for any line -> `409`, order `CANCELLED`, no stock reserved for any line.
* Concurrent orders for the last units: total shipped never exceeds stock.
* Same `Idempotency-Key` twice -> same order returned, stock reduced once.

Endpoints: `POST /api/orders`, `GET /api/orders/{id}`, `GET /api/inventory/items/{sku}`

### US-2 Know what happened to an order
*As a support analyst, I want to see every step the system took for an order, including compensation, so that I can explain failures.*
**Acceptance**
* `GET /api/orders/{id}/saga` lists `RESERVE_STOCK`, `POST_LEDGER`, `SHIP` with outcome and detail.
* A failed step shows `FAILED`, then `RELEASE_STOCK` / `CANCEL_LEDGER_EVENT` as `COMPENSATED`; the order is `CANCELLED` with a reason.
* If the ledger event was already published, a `COMPENSATION_REQUIRED` row flags it for follow-up.

Endpoints: `GET /api/orders/{id}`, `GET /api/orders/{id}/saga`, `GET /api/inventory/reservations/{orderId}`

### US-3 Replenish stock
*As a purchasing officer, I want to be told what is running low and raise a purchase order, so that we do not stock out.*
**Acceptance**
* `GET /api/inventory/low-stock` lists SKUs with `available <= reorderPoint`.
* `POST /api/purchase-orders/reorder-suggestions` creates a `DRAFT` PO for them (qty = reorder qty, cost = standard cost).
* `approve` then `receive` moves the PO to `RECEIVED` and adds the quantities to `on_hand`; receiving twice or cancelling a received PO -> `409`.

Endpoints: `GET /api/inventory/low-stock`, `POST /api/purchase-orders`, `POST /api/purchase-orders/reorder-suggestions`, `POST /api/purchase-orders/{id}/approve|receive|cancel`

### US-4 Orders show up in finance
*As a finance controller, I want every shipped order to be ledgered exactly once, so that revenue figures can be audited.*
**Acceptance**
* A `SHIPPED` order has a matching entry at `GET http://localhost:8090/api/ledger/orders/{orderId}` (ledger-service) shortly after, with the same totals and lines.
* Re-delivery of the event never creates a second ledger entry (idempotent on `eventId`).
* Orders cancelled by the saga before publication never reach the ledger.

Endpoints: `GET /api/orders/{id}` (ERP) and `GET /api/ledger/orders/{orderId}` (ledger-service)
