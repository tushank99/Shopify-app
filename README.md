

<h1 align="center">Distributed Event-Driven Recommendation Platform</h1>

<p align="center">
  <em>
    A production-grade <strong>distributed recommendation platform</strong> built around an
<strong>event-driven microservices architecture</strong>.<br/><br/>

The system decouples <strong>customer-facing APIs</strong> from computationally intensive
<strong>SVD-based recommendation pipelines</strong> using asynchronous processing,
enabling <strong>scalable, low-latency product services</strong> alongside
<strong>personalized ML inference</strong>.<br/><br/>

Every checkout is also streamed through a <strong>transactional outbox</strong> and <strong>Apache Kafka</strong>
into a <strong>Spring Boot + PostgreSQL ledger</strong>, with <strong>at-least-once delivery and idempotent ledger effects</strong>.
  </em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Node.js-339933?style=for-the-badge&logo=nodedotjs&logoColor=white" />
  <img src="https://img.shields.io/badge/Express-000000?style=for-the-badge&logo=express&logoColor=white" />
  <img src="https://img.shields.io/badge/React-61DAFB?style=for-the-badge&logo=react&logoColor=black" />
  <img src="https://img.shields.io/badge/MongoDB-47A248?style=for-the-badge&logo=mongodb&logoColor=white" />
  <img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white" />
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" />
  <img src="https://img.shields.io/badge/BullMQ-E34F26?style=for-the-badge&logo=redis&logoColor=white" />
  <img src="https://img.shields.io/badge/Surprise_SVD-FF6F00?style=for-the-badge&logo=scikitlearn&logoColor=white" />
</p>

---

## Table of Contents

- [The Engineering Problem](#the-engineering-problem)
- [Phased Evolution Strategy — The Order Event Pipeline](#phased-evolution-strategy--the-order-event-pipeline)
- [Delivery Guarantees](#delivery-guarantees)
- [System Architecture](#system-architecture)
- [Core System Capabilities](#core-system-capabilities)
- [Technical Deep Dives — The Debug Chronicles](#technical-deep-dives--the-debug-chronicles)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Environment Variables](#environment-variables)
- [API Reference](#api-reference)

---

## The Engineering Problem

Every major e-commerce platform faces the same fundamental systems design challenge: the **storefront** demands instant page loads with reliable database reads, while the **recommendation engine** needs to run computationally intensive matrix algebra — extracting latent user preference vectors from sparse rating matrices via SVD decomposition.

If both workloads share the same execution thread and the same database connection pool, a single heavy recommendation recalculation can spike CPU for hundreds of milliseconds and block every other user's product page from loading.

This project solves that tension by **decoupling** the two concerns into separate service boundaries connected via an asynchronous event bus, dedicated caching layers, and isolated connection pools — the same architectural pattern used at companies like Amazon and Netflix to run ML inference alongside transactional APIs.

### The second problem: the dual write

An order must reach two systems: MongoDB (the order itself) and Kafka (the event other services consume). These two writes cannot be one atomic step:

- Save the order, then crash before publishing → the order exists but the event is **lost**.
- Publish first, then the order transaction fails → the event describes an order that **never existed**.

The fix is a **transactional outbox**. The event is written as a row in the *same* MongoDB transaction as the order, so both commit or both roll back. A separate relay then moves outbox rows to Kafka. A crash can only cause a *re-send*, never a loss, and the consumer is built to absorb re-sends.

---

## Phased Evolution Strategy — The Order Event Pipeline

The order event pipeline was built in six phases. Each phase adds one layer and states the guarantee it provides.

```
POST /api/orders ──► Mongo txn { orders + outboxes (PENDING) }
                                   │
                     outbox relay (lease + fencing token)
                                   ▼
                  Kafka  order.placed  (3 partitions, key = userId)
                                   ▼
              ledger-service (Spring Boot) ──► PostgreSQL ledger_entry / ledger_item
                   │ transient failure                 │ invalid event
                   ▼                                   ▼
          order.placed-retry-0..4  ── exhausted ──►  order.placed-dlt  ──► dlt-redrive.mjs
```

### Phase 1 → Infrastructure, Node Outbox & Relay

**Goal:** Make the order and its event one atomic unit, then deliver the event to Kafka without ever losing it.

**What was built:**
- **Local stack** in `docker-compose.infra.yml`: MongoDB as a single-node replica set (needed for transactions), Kafka in KRaft mode, PostgreSQL, Redis and Jaeger. Topics are pre-created by an init container and **broker auto-create is off**, so a typo in a topic name fails loudly instead of creating a silent new topic.
- **Outbox write** (`backend/outbox/orderOutbox.js`): the order and its `order.placed` outbox row are saved in **one Mongo transaction** (`writeConcern: majority`). The `eventId` (UUID) is generated once, outside the transaction callback, so a transparent transaction retry can never produce two event IDs for one order.
- **Exact money:** prices are converted to integer minor units before building the event (`backend/events/orderPlaced.js`), so `0.1 × 3` is exactly `0.30`. An event that cannot be represented faithfully aborts the order instead of writing a wrong ledger.
- **Relay** (`backend/workers/outboxRelay.js`): a standalone process that claims one row at a time with `findOneAndUpdate` (atomic per document), a **lease** (`lockedAt`) and a **fencing token** (`lockToken`). Every later state change is conditional on that token (`backend/outbox/outboxStore.js`). Messages are published with `acks=-1`, keyed by `userId`, using the murmur2 partitioner so key → partition matches the Java client.
- **Error classification** (`backend/outbox/kafkaErrors.js`): *connectivity* errors (broker down, missing leader, timeout) never count as attempts and pause claiming. *Permanent* errors (oversized or unserialisable record) go straight to `FAILED`. *Unknown* errors retry with exponential backoff plus jitter, and become `FAILED` after `OUTBOX_MAX_ATTEMPTS`.
- **Operations:** `/healthz` and `/livez` on port 8085, a TTL index that removes `PUBLISHED` rows after 7 days, and admin endpoints for outbox stats and requeueing `FAILED` rows.

**Why it matters:** a Kafka outage never blocks checkout. Orders keep returning `201`, rows wait as `PENDING`, and the relay drains them when the broker returns.

---

### Phase 2 → Ledger Service Foundation

**Goal:** A separate Java service that owns the financial record of every order, with correctness enforced by the database, not by application code.

**What was built:**
- **Build** (`ledger-service/pom.xml`): Spring Boot 3.5 on Java 21. The Maven build copies the shared contract `contracts/order.placed.v1.schema.json` onto the classpath, so the Node producer and the Java consumer validate against **the same file**.
- **Config** (`application.yml`): manual offset commits (`enable-auto-commit: false`), `auto-offset-reset: earliest`, raw `ByteArrayDeserializer` values, and a Hikari pool sized above listener concurrency.
- **Schema** (Flyway `V1__init_ledger.sql`): `ledger_entry` and `ledger_item` with two independent guards, `NUMERIC(14,2)` money (never floating point), and `CHECK` constraints on ID lengths, currency, quantity and non-negative amounts.
- **Entities & repositories:** JPA entities are read-only, and writes are native `INSERT … ON CONFLICT DO NOTHING`. Hibernate `ddl-auto` is `none`, so Flyway is the only owner of the schema.
- **Dockerfile:** multi-stage build (Maven → JRE Alpine), runs as a non-root user, `MaxRAMPercentage=75`, and the OpenTelemetry Java agent is baked in.

**Why it matters:** the database itself rejects a duplicate or malformed ledger row, even if a bug slips through the code.

---

### Phase 3 → Event Validation, Idempotent Ingestion, Retry & DLT

**Goal:** Consume every event exactly once *in effect*, isolate bad messages, and survive downstream outages.

**What was built:**
- **Validation from raw bytes** (`OrderPlacedEventValidator`): the payload is parsed and checked against the JSON Schema, and the line totals must add up to `totalPrice`. A generic JSON deserializer would silently turn `{"corrupt":true}` into an object full of nulls. Validating the raw tree rejects it immediately.
- **Idempotent ingestion** (`LedgerService`): one transaction writes the entry and its lines. The result is `INSERTED`, `DUPLICATE_EVENT` or `DUPLICATE_ORDER`.
- **Kafka listener with retry topics** (`OrderPlacedListener`): `@RetryableTopic` with 6 attempts → `order.placed-retry-0..4` with exponential backoff (2 s × 2, capped at 30 s, about 60 s in total) → `order.placed-dlt`. Invalid events are **excluded** from retries, so a poison pill never blocks a partition.
- **Per-record commits** (`ack-mode: record`) and Micrometer counters: `ledger_events_ingested_total`, `ledger_events_duplicate_total{reason}`, `ledger_events_invalid_total{reason}` and `ledger_events_dlt_total`.
- **DLT redrive** (`scripts/dlt-redrive.mjs`): replays dead-lettered events back to `order.placed` once the root cause is fixed.

**Why it matters:** one bad message or a database restart never stops the pipeline, and replaying events is always safe.

---

### Phase 4 → REST API & Actuator

**Goal:** Read access to the ledger, plus production-style health and metrics endpoints.

**What was built:**
- `GET /api/ledger/orders/{orderId}`: returns the ledger entry with its lines. It returns `404` until the event has been consumed, because the pipeline is **eventually consistent**.
- `GET /api/ledger/users/{userId}?page=&size=`: newest first, backed by the index `(user_id, order_created_at DESC, event_id DESC)` for stable pagination. `size` is capped at 100, and a user with no entries gets an empty `200` page.
- Input checks: IDs must be 24-character hex, are matched case-insensitively and are stored lower-case. Errors go through a central `ApiExceptionHandler`.
- **Actuator on a separate port (8081):** `/actuator/health` with liveness and readiness groups (readiness includes the database) and `/actuator/prometheus`. The API port is bound to loopback (`127.0.0.1:8090`) and the management port is not published.

**Why it matters:** metrics and probes stay off the public port, and readiness reflects real database health.

---

### Phase 5 → Tests & CI

**Goal:** Prove the guarantees with tests that run against real infrastructure, on every push.

**What was built:**
- **Node (Vitest, `backend/__tests__/`)** against a real MongoDB replica set:
  - concurrent claims never return the same row
  - expired leases are reclaimed, fresh ones are not
  - a stale relay can never overwrite a reclaimed or published row
  - connectivity errors never consume attempts
  - order and outbox commit and roll back **together**
  - money is exact for float-hostile prices
- **Java (`LedgerIntegrationTest`)** with **EmbeddedKafka (3 partitions)** and **Testcontainers PostgreSQL**:
  - the happy path writes entry and lines in one transaction
  - a duplicate `eventId` is ignored
  - the same event delivered concurrently creates exactly one row
  - the same order under a new `eventId` is stopped by the second guard
  - the three poison-pill types go straight to the DLT while the next order still processes
- **GitHub Actions** (`.github/workflows/ci.yml`): two parallel jobs (Node + Java), `concurrency` cancels outdated runs, dependency caching is on, and surefire reports are uploaded when a run fails.

**Why it matters:** the tricky cases (races, duplicates, poison pills) are tested, not just the happy path.

---

### Phase 6 → Load Testing, Failure Drills & OpenTelemetry

**Goal:** Break the system on purpose and show that the guarantees hold, and make every order traceable end to end.

**What was built:**
- **k6 load** (`benchmark/k6-drill-orders.js`): places an exact number of orders spread across seeded users, and writes the counts that the drill runner reconciles against. `benchmark/k6-final-test.js` is a ramping stress test with a p95 checkout latency threshold.
- **Failure drill runner** (`scripts/drills/drills.mjs`): 9 automated drills that kill, stop and duplicate parts of the system, then compare Mongo, Kafka, Postgres and metric deltas against a baseline. Results are written as a Markdown table. See [Delivery Guarantees](#delivery-guarantees).
- **Distributed tracing:** Node uses the OpenTelemetry SDK (`backend/otel.js`, ESM loader hook) and Java uses the OTel agent, all exporting to Jaeger. The HTTP request's trace context is stored on the outbox row (`traceCarrier`) and restored by the relay, so **one trace** covers `POST /api/orders` → Mongo transaction → relay publish → Kafka → ledger consume → JDBC insert, even though the publish happens seconds later in another process.

**Why it matters:** the reliability claims in this README are backed by drills you can re-run, not by assumption.

---

## Delivery Guarantees

### Delivery model

**At-least-once delivery with idempotent ledger effects.** Kafka may deliver an event more than once, and the ledger makes the second delivery a no-op. The result is an **exactly-once ledger effect per `(order_id, event_type)`**.

### Two guards, two different protections

| Guard | What it stops | Outcome |
|-------|---------------|---------|
| `event_id` **PRIMARY KEY** + `INSERT … ON CONFLICT DO NOTHING` | The **same event** redelivered, replayed or redriven | `DUPLICATE_EVENT`: no-op, offset committed |
| `UNIQUE (order_id, event_type)` | The **same business order** arriving under a *different* `event_id` | `DUPLICATE_ORDER`: transaction rolled back, logged and counted |

### Keys and ordering

- Messages are keyed by **`userId`**, so all events for one user land on the same partition. The key only drives partitioning: the payload is authoritative, and a key/payload mismatch is logged, not failed.
- **Ordering between events is not required.** Each `order.placed` event is self-contained and the ledger write is idempotent.
- Retry topics *can* reorder events: a later order may be ledgered while an earlier one is still waiting in `retry-N`. This is safe by design, because no ledger row depends on another.

### Failure classification

| Case | Route |
|------|-------|
| Invalid JSON, schema violation, totals that don't add up | **Straight to `order.placed-dlt`**, no retries |
| Transient failure (Postgres down, deadlock) | `order.placed-retry-0..4` with exponential backoff |
| Duplicate event or duplicate order | **Successful no-op**: counted, offset committed |
| Retries exhausted (about 60 s) | `order.placed-dlt` |

### DLT redrive

`scripts/dlt-redrive.mjs` republishes dead-lettered records to `order.placed`:
- **The original key and value bytes are sent unchanged**, so the `event_id` is preserved and both guards still apply. Redriving twice is harmless.
- **The original headers are kept** (`event-id`, `traceparent`, `content-type`), so the replayed event joins its original trace. Spring's `kafka_*` DLT headers are dropped, and a `redriven-from: <dlt>:<partition>:<offset>` header is added.
- Poison pills are skipped by default, because replaying them only sends them back to the DLT. The options are `--dry-run`, `--since <epoch-ms>`, `--max <n>` and `--include-invalid`.

### The outbox crash window

This is the core of the at-least-once → exactly-once-effect story:

1. The relay claims an outbox row and publishes the event to Kafka.
2. The relay crashes **before** `markPublished`, so the row stays `PROCESSING`.
3. The lease expires and another relay reclaims the row with a new fencing token.
4. The event is **published again**: Kafka now holds two copies with the same `event_id`.
5. The ledger inserts the first copy and treats the second as `DUPLICATE_EVENT`, leaving **one row**.

If the first relay later wakes up, its `markPublished` matches nothing because its token is stale (see [the fencing deep dive](#why-the-relay-needs-a-fencing-token)).

### Failure drills

Run with `node scripts/drills/drills.mjs all`. Each drill checks deltas against a baseline, so clean volumes are not needed.

| # | Drill | Pass criteria |
|---|-------|---------------|
| 1 | Volume: 100 orders | k6 `201` count = Mongo `PUBLISHED` rows = `ledger_entry` rows |
| 2 | Crash recovery: `docker kill` the ledger mid-stream | Rebalance completes, `ledger_entry` ends exactly at the order count, 0 DLT records |
| 3 | Duplicate replay: offsets shifted back 10 per partition | Duplicates ignored = events replayed, `ledger_entry` count unchanged |
| 4 | Poison pills: invalid bytes + valid JSON with invalid schema | Both reach the DLT without retries, and later valid orders still process |
| 5 | Broker outage: `docker stop kafka`, 20 orders | HTTP `201` throughout, the outbox drains to `PUBLISHED` after restart, 0 `FAILED` rows |
| 6 | Two relays on one backlog | 100 distinct event IDs, no database errors |
| 7 | Relay `kill -9` between send and mark | N + 1 messages on the topic, N distinct IDs, still one ledger row per order |
| 8 | Postgres outage of 20 s (inside the retry window) | Retries absorb it, 0 DLT records, every order ledgered |
| 8b | Postgres outage of 100 s (beyond the retry window) | Events reach the DLT, the redrive restores every ledger row, 0 `FAILED` rows |

> **Note on diagrams:** The architecture diagrams below show the *target vision* for this platform's evolution. Components such as Two-Tower Neural Networks, MLflow Model Registry, and Vector DB retrieval are shown for direction and are **not part of the current codebase**. Everything documented in the phases above reflects actual working code.

---

## System Architecture

### End-to-End Data Flow

The system is composed of three independently deployable services connected via Redis-backed message queues:

<p align="center">
  <img src="docs/system-architecture-flow.png" alt="End-to-End System Architecture" width="400" />
</p>

**How a request flows through the system:**

1. **React Frontend** → Makes API calls to a single HTTPS endpoint
2. **Node.js API Gateway** → Authenticates via JWT cookies, routes to internal controllers, returns transactional data (products, orders) directly from MongoDB
3. **Async Event Dispatch** → On checkout or review submission, the controller drops a job payload into a BullMQ queue and returns `201` to the shopper immediately
4. **BullMQ Worker** → Picks up the job from Redis, sends interaction data to the Python service, and hot-swaps the user's Redis recommendation cache
5. **Python FastAPI Sidecar** → Maintains the SVD model in memory, accepts matrix update events, runs periodic retraining via APScheduler, and serves real-time recommendation predictions
6. **Redis Cache** → Serves recommendation data at sub-millisecond latency on cache hits, bypassing both MongoDB and the Python service entirely

### Offline Pipeline vs. Online Serving

<p align="center">
  <img src="docs/offline-online-pipeline.png" alt="Offline vs Online Pipeline" width="90%" />
</p>

| Pipeline | What Happens | Implemented |
|----------|-------------|-------------|
| **Online Serving** | `Request → API Gateway → Redis Cache Hit? → (Yes) Return instantly / (No) → FastAPI Inference → SVD Prediction → Rank & Filter → Save to Redis → Return` |  Fully implemented |
| **Offline Pipeline** | `User Interactions → Data Engineering (MongoDB Aggregation) → SVD Model Training → Store in Memory → Scheduled Retraining via APScheduler` | SVD training & scheduled retraining implemented |
| **Future Offline** | `Two-Tower Deep Model → Model Evaluation (Recall@K, NDCG) → MLflow Model Registry` |  Planned |

---

## Core System Capabilities

###  The Systems Engineering Layer

<table>
<tr>
<th width="30%">Capability</th>
<th width="70%">Implementation Detail</th>
</tr>
<tr>
<td><strong>Transactional Outbox</strong></td>
<td>The order and its <code>order.placed</code> event row commit in one MongoDB transaction (<code>w: majority</code>). There is no dual write, so an order and its event can never disagree.</td>
</tr>
<tr>
<td><strong>Lease + Fencing Relay</strong></td>
<td>Rows are claimed atomically with a lease and a per-claim <code>lockToken</code>. Any number of relays can run in parallel. A relay whose lease expired can no longer change the row, because every write is conditional on its token.</td>
</tr>
<tr>
<td><strong>Idempotent Ledger</strong></td>
<td>PostgreSQL enforces an exactly-once effect per <code>(order_id, event_type)</code> with two guards: the <code>event_id</code> primary key and a <code>UNIQUE (order_id, event_type)</code> constraint. Money is stored as <code>NUMERIC</code>, never as floating point.</td>
</tr>
<tr>
<td><strong>Retry Topics, DLT & Redrive</strong></td>
<td>Transient failures retry on <code>order.placed-retry-0..4</code> with exponential backoff without blocking the main partition. Invalid events skip retries and go to <code>order.placed-dlt</code>. A redrive script replays the DLT with the original bytes and trace headers.</td>
</tr>
<tr>
<td><strong>Contract-First Events</strong></td>
<td>One JSON Schema (<code>contracts/order.placed.v1.schema.json</code>) is enforced by the Node producer (AJV) and by the Java consumer. Amounts travel as decimal strings, and the consumer checks that line totals add up.</td>
</tr>
<tr>
<td><strong>Distributed Tracing</strong></td>
<td>OpenTelemetry in Node (SDK) and Java (agent), exported to Jaeger. The trace context is persisted on the outbox row, so one trace spans the HTTP request, the async relay and the Kafka consumer.</td>
</tr>
<tr>
<td><strong>Backend-for-Frontend (BFF) Gateway</strong></td>
<td>Node.js Express server acts as the single API surface. The React frontend only communicates with <code>/api/*</code> — it has zero knowledge of MongoDB, Redis, or the Python service. Internal service topology can be refactored without any frontend changes.</td>
</tr>
<tr>
<td><strong>Event-Driven Asynchrony</strong></td>
<td>After the order commits, BullMQ jobs run the follow-up work off the request path: <code>stock-decrement</code> (idempotent per order via a unique <code>processedStockOrder</code> record), <code>ml-recommendation</code> (purchase signal to the ML service and cache invalidation) and <code>recommendation-updates</code> (review → cache hot-swap). Retries use exponential backoff (<code>attempts: 3</code>) and workers run with <code>concurrency: 5</code>.</td>
</tr>
<tr>
<td><strong>Atomic Stock Reservation</strong></td>
<td>A Redis Lua script checks and decrements stock in one atomic step, so two concurrent checkouts can never both take the last unit. Reservations are released if the order transaction fails.</td>
</tr>
<tr>
<td><strong>Cache Stampede Prevention</strong></td>
<td>Redis <code>SET NX</code> mutex locks ensure that when a hot cache key expires under high concurrency, only one request rebuilds the cache. Other concurrent requests receive a graceful fallback (top-rated products) instead of all hammering the Python sidecar simultaneously.</td>
</tr>
<tr>
<td><strong>Sliding-Window Rate Limiting</strong></td>
<td>Redis sorted-set rate limiter (per user, or per IP when anonymous) on order creation, executed as one <code>MULTI</code> block and designed to fail open if Redis is unreachable.</td>
</tr>
<tr>
<td><strong>Connection Isolation</strong></td>
<td>The Redis module exports a <code>createRedisConnection()</code> factory — each BullMQ worker daemon gets its own dedicated TCP socket, preventing the TLS socket hijacking crisis documented below.</td>
</tr>
<tr>
<td><strong>Optimized Database Layer</strong></td>
<td>Three MongoDB indexes (compound <code>{category, price}</code>, sort <code>{createdAt: -1}</code>, weighted text on name/brand/description) bring query patterns from <code>COLLSCAN</code> to <code>IXSCAN</code> — verified via the built-in <code>/explain-query</code> diagnostic endpoint.</td>
</tr>
<tr>
<td><strong>Image Pipeline</strong></td>
<td>Dual-mode upload system: Cloudinary CDN in production (with automatic 800×800 resize transforms), local disk storage as development fallback. The upload route dynamically detects which backend is available.</td>
</tr>
<tr>
<td><strong>Authentication & Authorization</strong></td>
<td>JWT tokens stored in HTTP-only cookies. Gateway-level middleware enforces authentication before controllers execute. Admin-specific routes require an additional <code>isAdmin</code> role check.</td>
</tr>
</table>

---

###  The Modeling Layer

<table>
<tr>
<th width="30%">Capability</th>
<th width="70%">Implementation Detail</th>
</tr>
<tr>
<td><strong>SVD Matrix Factorization</strong></td>
<td>Uses <code>surprise.SVD</code> to decompose the sparse user-product rating matrix into low-rank latent factor matrices. The algorithm finds hidden features (latent factors) that explain observed ratings — for example, a user who rates many electronics highly will have latent factor values that align with products in that space, even for products they haven't seen yet.</td>
</tr>
<tr>
<td><strong>Server-Side Aggregation Pipeline</strong></td>
<td>Instead of pulling entire product documents and parsing reviews in Python, a MongoDB aggregation pipeline (<code>$match → $unwind → $project</code>) extracts flat <code>{user, item, rating}</code> tuples directly at the database layer — reducing network payload from nested document arrays to minimal row-based data.</td>
</tr>
<tr>
<td><strong>Cold-Start Gate</strong></td>
<td>Custom threshold logic: <code>if len(ratings_data) < 5</code>, SVD training is skipped entirely to prevent null matrix decomposition errors. On the API side, users with no review history are detected via <code>Product.findOne({"reviews.user": userId})</code> and served a popularity-based fallback.</td>
</tr>
<tr>
<td><strong>Live Interaction Matrix Injection</strong></td>
<td>Purchase events streamed via BullMQ are injected into the in-memory Pandas DataFrame as implicit high-confidence ratings (score 5.0). Duplicate order events are ignored using a bounded set of processed order IDs. The next scheduled retraining cycle incorporates recent purchase behavior without requiring a full MongoDB re-extraction.</td>
</tr>
<tr>
<td><strong>Automated Batch Retraining</strong></td>
<td>APScheduler runs two concurrent schedules: a production cron job (daily at 3 AM) and a demo interval (every 2 minutes). Both trigger a full SVD <code>.fit()</code> on the current DataFrame, atomically swapping the global model reference. A manual override is exposed via <code>POST /retrain</code>.</td>
</tr>
<tr>
<td><strong>Prediction Serving</strong></td>
<td>For a given user, the model predicts estimated ratings for every product in the catalog, sorts by predicted score, and returns the top 10 IDs. The Node.js gateway then fetches the full product documents from MongoDB, preserving the SVD-predicted ranking order.</td>
</tr>
</table>

---

## Technical Deep Dives — The Debug Chronicles

###  Why the relay needs a fencing token

**The problem:** a lease alone is not enough. Picture relay A claiming a row, then pausing for longer than the lease (GC pause, network stall). Relay B sees an expired lease, reclaims the row and publishes it. Then A wakes up and runs its own `markPublished` or `markFailedAttempt`. Without protection, A overwrites B's result. It could even push an already-published row back to `PENDING`.

**The fix:** every claim writes a fresh random `lockToken`. Every later transition only matches the row if the status is still `PROCESSING` **and** the token is still the one this relay wrote:

```javascript
// backend/outbox/outboxStore.js
const fence = (row) => ({
  _id: row._id,
  status: OUTBOX_STATUS.PROCESSING,
  lockToken: row.lockToken,
});

const res = await Outbox.updateOne(fence(row), { $set: { status: "PUBLISHED", publishedAt: now } });
return res.modifiedCount === 1; // false => this relay lost the row; it does nothing
```

**Result:** a stale relay's write matches zero documents and is dropped. The worst case is a duplicate Kafka message, which the ledger already absorbs. This is covered by the Vitest suite ("a stale relay cannot overwrite a row that another relay re-claimed") and by drills 6 and 7.

---

###  The Database Optimization Pivot

**The problem:** The original product search used a `$regex`-based query pattern:

```
{ name: { $regex: keyword, $options: "i" } }
```

This forces MongoDB to perform a **full collection scan** (`COLLSCAN`) on every search query — the database must read every single document and apply the regex pattern against the `name` field of each one. As the product catalog grows, this becomes linearly slower.

**The diagnosis:** Using MongoDB's `.explain("executionStats")`, the query planner confirmed `COLLSCAN` as the winning plan stage, with `totalDocsExamined` equal to the entire collection size even for simple keyword searches.

**The fix:** Replaced `$regex` with MongoDB's native `$text` search operator, backed by a weighted compound text index:

```
productSchema.index(
  { name: "text", description: "text", brand: "text" },
  { weights: { name: 10, brand: 5, description: 1 } }
);
```

The `$text` operator pushes tokenization, stemming, stop-word removal, and relevance scoring entirely to the Atlas cluster's C++ engine. The weighted configuration ensures a product name match (`weight: 10`) is ranked 10× higher than a description match (`weight: 1`), producing more intuitive search results.

The optimized query now uses `IXSCAN` → `TEXT_MATCH` stages, with `totalDocsExamined` proportional only to matching documents rather than the entire collection. A built-in diagnostic endpoint (`GET /api/products/explain-query?keyword=...`) lets you verify this in real-time.

---

###  Technical Case Study: Resolving Asynchronous Connection Hijacking & Socket Collision over Cloud TLS

#### The Production Crisis

During high-concurrency staging deployment on Railway, the Node.js application container experienced immediate degradation, throwing persistent **502 Bad Gateway** errors to incoming web traffic. Terminal streams revealed a cascading failure loop characterized by rapid, cyclical `Connected → Closed → Reconnecting` events, accompanied by explicit network faults:

```
Redis Connection Error: read ECONNRESET
Error: read ECONNRESET
    at TCP.onStreamRead (node:internal/stream_base_commons:216:20) {
      errno: -104,
      code: 'ECONNRESET',
      syscall: 'read'
    }
```

#### Root-Cause Analysis

The system architecture relies on BullMQ for decoupling transactional storefront operations from downstream analytical computing tasks. In the initial design, a **single shared `ioredis` client instance** was initialized as a singleton object and exported across the entire codebase to handle:

- Application-layer cache reads/writes (`redis.get()`, `redis.set()`)
- BullMQ queue definitions
- BullMQ background worker routines

When the application was pushed to production using an encrypted cloud infrastructure provider (Upstash via a secure `rediss://` TLS endpoint), the following failure chain was triggered:

1. **Blocking Polling Mechanism:** BullMQ background Worker processes inherently run continuous, long-polling atomic commands — specifically `BRPOPLPUSH` or `BLMOVE` — against the Redis cluster to instantly ingest incoming stream payload data.

2. **Socket Hijacking:** Because the singleton database connection instance was passed directly to the worker (`{ connection: redis }`), the worker's blocking polling loop completely **monopolized and locked down the single underlying TCP socket**.

3. **Cross-Talk Collision:** When parallel client routing threads concurrently executed distinct database commands (such as clearing user recommendation arrays via `redis.del()`), they attempted to **write directly over the exact same encrypted TLS stream channel**.

4. **Cloud-Side Termination:** The secure remote Redis cluster interpreted this erratic, concurrent command cross-talk on a single secure socket line as an **invalid state protocol violation**. To protect network integrity, the cloud database controller forcefully dropped the TCP socket link via an external `ECONNRESET` signal.

This crash broke the Node event loop and prevented the server from responding to Railway's public web proxy, dropping the storefront into a **502 failure state**.

#### The Engineering Solution

The Redis configuration module was refactored from a singleton client pattern to an **isolated Connection Factory pattern**:

```javascript
// redis.js — Isolated Configuration Factory

export const redisConfig = {
  maxRetriesPerRequest: null,  // Required by BullMQ
  tls: needsTls ? { rejectUnauthorized: false } : undefined,
  connectTimeout: 10000,
  retryStrategy: (times) => Math.min(times * 200, 5000),
};

export const createRedisConnection = () =>
  process.env.REDIS_URL
    ? new Redis(process.env.REDIS_URL, redisConfig)
    : new Redis({ host: "127.0.0.1", port: 6379, ...redisConfig });

// Main standalone client — only for API cache get/set
const redis = createRedisConnection();
export default redis;
```

By changing the instantiation inside every queue module (`stockDecrementQueue.js`, `mlRecommendationQueue.js`, `recommendationQueue.js`) to ingest the **raw config factory** instead of the client pointer, each BullMQ worker daemon operates on its own dedicated TCP socket:

```javascript
// stockDecrementQueue.js — Isolated Connection Per Queue and Worker

export const stockDecrementQueue = new Queue("stock-decrement", {
  connection: createRedisConnection(),  // ← Dedicated socket
});

const stockWorker = new Worker("stock-decrement", async (job) => {
  // ... async processing
}, {
  connection: createRedisConnection(),  // ← Another dedicated socket
  concurrency: 5,
});
```

**Result:** Isolated Redis connections now run in parallel — one for the main API cache, one for each BullMQ queue, and one for each worker — eliminating all cross-talk on the TLS channel. The 502 failures resolved immediately and the system maintained zero-downtime stability under high-concurrency load.

---

## Project Structure

```
├── .github/workflows/
│   └── ci.yml                    # CI: Node outbox tests + Java ledger tests in parallel
│
├── api/
│   └── index.js                  # Vercel serverless entry: re-exports the Express app
│
├── backend/
│   ├── __tests__/
│   │   ├── outbox.test.js        # Claim, lease, fencing, failure handling, atomic order + outbox, contract
│   │   └── outboxConfig.test.js  # Outbox on/off gate (pure unit tests)
│   ├── config/
│   │   ├── db.js                 # MongoDB connection with reconnection guard
│   │   └── redis.js              # Redis factory + Lua stock-reservation script
│   ├── controllers/
│   │   ├── productController.js  # CRUD + text search + recommendations + mutex lock + explain query
│   │   ├── orderController.js    # Stock reservation → order + outbox transaction → BullMQ jobs
│   │   ├── queueController.js    # DLQ replay, outbox stats, outbox requeue
│   │   ├── userController.js     # Auth + cache eviction on logout
│   │   └── categoryController.js # Category CRUD
│   ├── events/
│   │   └── orderPlaced.js        # Builds + validates the order.placed v1 event (exact minor-unit money)
│   ├── middlewares/
│   │   ├── authMiddleware.js     # JWT cookie verification + admin role gate
│   │   ├── rateLimiterMiddleware.js # Redis sliding-window rate limiter
│   │   ├── asyncHandler.js       # Express async error wrapper
│   │   ├── checkId.js            # MongoDB ObjectId format validator
│   │   └── errorMiddleware.js    # Global 404 & error response formatting
│   ├── models/
│   │   ├── productModel.js       # Product schema + review subdoc + 3 MongoDB indexes
│   │   ├── orderModel.js         # Order schema with payment result tracking
│   │   ├── outboxModel.js        # Outbox rows: claim indexes + TTL on PUBLISHED
│   │   ├── processedStockOrderModel.js # Unique orderId → idempotent stock decrement
│   │   ├── userModel.js          # User schema with admin flag
│   │   └── categoryModel.js      # Category schema
│   ├── outbox/
│   │   ├── orderOutbox.js        # Saves order + outbox row in one transaction
│   │   ├── outboxStore.js        # Claim / lease / fencing state transitions
│   │   ├── outboxConfig.js       # Outbox on/off gate (KAFKA_BROKERS, OUTBOX_ENABLED)
│   │   └── kafkaErrors.js        # connectivity / permanent / unknown error classification
│   ├── queues/
│   │   ├── stockDecrementQueue.js    # BullMQ: idempotent MongoDB stock decrement
│   │   ├── mlRecommendationQueue.js  # BullMQ: purchase → ML matrix update → cache invalidation
│   │   ├── recommendationQueue.js    # BullMQ: review → cache hot-swap
│   │   └── orderQueue.js             # BullMQ: legacy order-events queue
│   ├── routes/
│   │   ├── productRoutes.js      # Product API routes including /recommendations
│   │   ├── orderRoutes.js        # Order API routes with admin-only listing
│   │   ├── queueRoutes.js        # Admin queue + outbox routes
│   │   ├── userRoutes.js         # User auth & profile routes
│   │   ├── categoryRoutes.js     # Category admin routes
│   │   └── uploadRoutes.js       # Cloudinary / local disk dual-mode image uploads
│   ├── scripts/
│   │   ├── fixProductImages.js   # Bulk image URL migration utility
│   │   └── migrateImages.js      # Cloudinary migration script
│   ├── utils/
│   │   └── createToken.js        # JWT token generator + HTTP-only cookie setter
│   ├── workers/
│   │   └── outboxRelay.js        # Standalone relay: outbox → Kafka, /healthz on :8085
│   ├── otel.js                   # OpenTelemetry bootstrap (ESM loader hook) for API + relay
│   ├── seeder.js                 # Database seeder: 50+ products, 20 users, review generation
│   └── index.js                  # Express app: CORS, routes, health checks, static serving
│
├── contracts/
│   └── order.placed.v1.schema.json   # Shared event contract (Node producer + Java consumer)
│
├── ledger-service/
│   ├── Dockerfile                # Multi-stage, non-root, OTel Java agent built in
│   ├── Dockerfile.dockerignore   # Keeps the repo-root build context small
│   ├── pom.xml                   # Spring Boot 3 / Java 21, copies contracts onto the classpath
│   └── src/
│       ├── main/java/com/shopify/ledger/
│       │   ├── api/              # LedgerController, DTOs, ApiExceptionHandler
│       │   ├── config/           # Kafka config (DLT template)
│       │   ├── domain/           # LedgerEntry, LedgerItem (read-only JPA entities)
│       │   ├── event/            # OrderPlacedEvent + schema validator
│       │   ├── kafka/            # OrderPlacedListener: retry topics + DLT handler
│       │   ├── repository/       # Native INSERT … ON CONFLICT + query repositories
│       │   └── service/          # LedgerService (idempotent ingest), LedgerQueryService
│       ├── main/resources/
│       │   ├── application.yml   # Kafka, datasource, actuator on :8081, retry settings
│       │   └── db/migration/V1__init_ledger.sql  # Ledger schema with both idempotency guards
│       └── test/java/com/shopify/ledger/
│           └── LedgerIntegrationTest.java  # EmbeddedKafka + Testcontainers Postgres
│
├── ml-backend/
│   ├── main.py                   # FastAPI: SVD training, /recommend, /update-matrix, APScheduler
│   └── requirements.txt          # Python deps: fastapi, surprise, pandas, pymongo, apscheduler
│
├── frontend/
│   └── src/
│       ├── main.jsx              # React router: public, private, admin route trees
│       ├── App.jsx               # Root layout with navigation
│       ├── redux/
│       │   ├── store.js          # Redux store: auth, favorites, cart, shop, RTK Query
│       │   ├── api/
│       │   │   ├── apiSlice.js   # RTK Query base with dynamic BASE_URL
│       │   │   ├── productApiSlice.js  # Product endpoints incl. recommendations
│       │   │   ├── orderApiSlice.js    # Order CRUD endpoints
│       │   │   ├── usersApiSlice.js    # Auth endpoints
│       │   │   └── categoryApiSlice.js # Category endpoints
│       │   └── features/         # Auth, cart, favorites, shop Redux slices
│       ├── pages/
│       │   ├── Home.jsx          # Landing: hero, trust badges, recommendations, product grid
│       │   ├── Shop.jsx          # Shop with category/price filtering
│       │   ├── Cart.jsx          # Shopping cart with quantity management
│       │   ├── Products/
│       │   │   ├── PersonalizedRecommendations.jsx  # ML-powered "Recommended For You" section
│       │   │   ├── ProductDetails.jsx  # Product page with reviews tab
│       │   │   ├── ProductTabs.jsx     # Review system with helpful votes
│       │   │   └── ...                 # Cards, carousel, ratings, favorites
│       │   ├── Orders/           # Shipping, place order, order tracking, success page
│       │   ├── Admin/            # Dashboard, user/product/order/category management
│       │   └── Auth/             # Login & registration forms
│       └── components/           # Shared: Header, Loader, Modal, PrivateRoute, ProgressSteps
│
├── benchmark/
│   ├── drill-seed.js             # Seeds drill users + JWTs + a high-stock product
│   ├── k6-drill-orders.js        # k6: exact N orders over M users, writes counts for the drills
│   ├── k6-final-test.js          # k6: ramping stress test with p95 latency threshold
│   └── seed-users.js             # Signs JWTs for seeded users (load-test auth data)
│
├── scripts/
│   ├── drills/drills.mjs         # 9 automated failure drills with a PASS/FAIL report
│   └── dlt-redrive.mjs           # Replays order.placed-dlt back to order.placed
│
├── docs/
│   ├── OUTBOX_KAFKA.md           # Pipeline runbook: config, tracing, operations, drills
│   ├── system-architecture-flow.png  # End-to-end system flow diagram
│   └── offline-online-pipeline.png   # Offline vs online pipeline diagram
│
├── docker-compose.infra.yml      # Mongo rs0, Kafka (KRaft) + topics, Postgres, Redis, Jaeger, ledger
├── package.json                  # Monorepo root: Node.js dependencies + workspace scripts
├── pnpm-workspace.yaml           # PNPM workspace: backend + frontend packages
├── vitest.config.js              # Vitest config for the outbox suite
└── vercel.json                   # Vercel build + API rewrite
```

---

## Getting Started

### Prerequisites

- **Node.js** ≥ 20.6 (needed for the OpenTelemetry ESM loader hook)
- **pnpm**
- **Python** ≥ 3.9
- **Docker** with Docker Compose (MongoDB replica set, Kafka, PostgreSQL, Redis, Jaeger)
- **Java 21** + Maven (only to build or test the ledger outside Docker)
- **k6** (only for load tests and drills)

### 1. Clone & Install

```bash
git clone https://github.com/tushank99/Shopify-app.git
cd Shopify-app

# Installs the monorepo root, backend and frontend workspaces
pnpm install
```

### 2. Start the Infrastructure

```bash
# MongoDB (replica set), Kafka + topics, PostgreSQL, Redis, Jaeger
docker compose -f docker-compose.infra.yml up -d

# Ledger service (Spring Boot) as a container
docker compose -f docker-compose.infra.yml --profile app up -d --build ledger-service
```

### 3. Set Up the Python ML Service

```bash
cd ml-backend

# Create and activate virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the project root (see [Environment Variables](#environment-variables) below).

### 5. Seed the Database (Optional)

```bash
cd backend
node seeder.js
```

This seeds 50+ products across 5 categories with 20 test users and realistic review distributions — providing enough rating data for the SVD model to train meaningfully.

### 6. Start All Services

```bash
# Terminal 1 — Node.js Backend (or: pnpm run backend:otel, with tracing)
pnpm run backend

# Terminal 2 — Outbox relay (or: pnpm run relay:otel, with tracing)
pnpm run relay

# Terminal 3 — React Frontend
pnpm run frontend

# Terminal 4 — Python ML Service
pnpm run ml
```

| Service | URL |
|---------|-----|
| React Frontend | `http://localhost:5173` |
| Node.js API Gateway | `http://localhost:5000` |
| Python ML Service | `http://localhost:8000` |
| ML Health Check | `http://localhost:8000/health` |
| Outbox Relay Health | `http://localhost:8085/healthz` |
| Ledger API | `http://127.0.0.1:8090/api/ledger` |
| Jaeger UI (traces) | `http://localhost:16686` |

### 7. Run the Tests

```bash
# Node outbox suite (needs the Mongo replica set from step 2)
pnpm run test:outbox

# Ledger service (EmbeddedKafka + Testcontainers, needs Docker)
cd ledger-service && mvn -B verify
```

### 8. Run the Failure Drills

```bash
node scripts/drills/drills.mjs seed     # drill users, JWTs and a high-stock product
node scripts/drills/drills.mjs all      # every drill, in order
node scripts/drills/drills.mjs 5 7      # or pick drills by number / key
```

The runner starts and stops its own relays, so stop any relay you started in step 6 first. The full runbook is in [docs/OUTBOX_KAFKA.md](docs/OUTBOX_KAFKA.md).

---

## Environment Variables

Create a `.env` file in the project root:

```env
# Server
PORT=5000
NODE_ENV=development

# MongoDB — local replica set from docker-compose.infra.yml (transactions need a replica set)
MONGO_URI=mongodb://127.0.0.1:27017/shopify?directConnection=true
# MongoDB — Atlas
# MONGO_URI=mongodb+srv://<username>:<password>@cluster.mongodb.net/<dbname>

# Authentication
JWT_SECRET=your_jwt_secret_key

# PayPal (Sandbox)
PAYPAL_CLIENT_ID=your_paypal_client_id

# Cloudinary (Image CDN)
CLOUDINARY_CLOUD_NAME=your_cloud_name
CLOUDINARY_API_KEY=your_api_key
CLOUDINARY_API_SECRET=your_api_secret

# Redis — Local Development
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
# Host port for the compose Redis, if 6379 is already taken
# REDIS_HOST_PORT=6379

# Redis — Production (Upstash)
# REDIS_URL=rediss://default:password@endpoint.upstash.io:6379

# Python ML Service URL (for BullMQ workers)
# ML_SERVICE_URL=http://127.0.0.1:8000

# Frontend URL (for CORS)
# FRONTEND_URL=https://your-frontend.vercel.app

# Kafka + outbox (the outbox is written only when KAFKA_BROKERS is set)
KAFKA_BROKERS=localhost:9092
# OUTBOX_ENABLED=true
# OUTBOX_LEASE_MS=30000          # claim lease
# OUTBOX_SEND_TIMEOUT_MS=8000    # must satisfy sendTimeout * 3 <= lease
# OUTBOX_BATCH_SIZE=100
# OUTBOX_IDLE_SLEEP_MS=300
# OUTBOX_MAX_ATTEMPTS=5          # only unknown errors count; broker outages never do
# OUTBOX_HEALTH_PORT=8085

# Ledger database (used by docker-compose.infra.yml)
# LEDGER_DB_USER=ledger
# LEDGER_DB_PASSWORD=ledger_pw

# OpenTelemetry
# OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
# OTEL_SDK_DISABLED=false
# LEDGER_OTEL_ENABLED=true       # Java agent switch in compose
```

The full relay and tracing tables are in [docs/OUTBOX_KAFKA.md §2](docs/OUTBOX_KAFKA.md#2-configuration).

---

## API Reference

### Authentication

| Method | Endpoint | Access | Description |
|--------|----------|--------|-------------|
| `POST` | `/api/users` | Public | Register a new user |
| `POST` | `/api/users/auth` | Public | Login with email/password |
| `POST` | `/api/users/logout` | Private | Logout + evict Redis recommendation cache |
| `GET` | `/api/users/profile` | Private | Get current user profile |
| `PUT` | `/api/users/profile` | Private | Update profile |

### Products

| Method | Endpoint | Access | Description |
|--------|----------|--------|-------------|
| `GET` | `/api/products` | Public | List products with `$text` search & pagination |
| `GET` | `/api/products/:id` | Public | Get product by ID |
| `GET` | `/api/products/top` | Public | Top 4 rated products |
| `GET` | `/api/products/new` | Public | 5 newest products |
| `GET` | `/api/products/recommendations` | Private | **SVD-powered personalized recommendations** |
| `POST` | `/api/products/:id/reviews` | Private | Submit review → triggers BullMQ recommendation update |
| `POST` | `/api/products/:id/reviews/helpful` | Private | Mark a review as helpful |
| `GET` | `/api/products/:id/can-review` | Private | Check if user can review (purchase verification) |
| `GET` | `/api/products/explain-query` | Public | MongoDB query planner diagnostic |
| `GET` | `/api/products/test-mutex` | Public | Redis mutex lock demonstration |
| `POST` | `/api/products/filtered-products` | Public | Filter by category + price range |

### Orders

| Method | Endpoint | Access | Description |
|--------|----------|--------|-------------|
| `POST` | `/api/orders` | Private | Create order: stock reservation + order and outbox row in one transaction + BullMQ jobs (rate limited) |
| `GET` | `/api/orders/mine` | Private | Get current user's orders |
| `GET` | `/api/orders/:id` | Private | Get order by ID |
| `PUT` | `/api/orders/:id/pay` | Private | Mark order as paid |
| `PUT` | `/api/orders/:id/deliver` | Admin | Mark order as delivered |
| `GET` | `/api/orders` | Admin | List all orders |
| `GET` | `/api/orders/total-orders` | Public | Total order count |
| `GET` | `/api/orders/total-sales` | Public | Total sales |
| `GET` | `/api/orders/total-sales-by-date` | Public | Paid sales grouped by day |

### Queues & Outbox (Admin)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/queues/retry-failed` | Retry failed BullMQ jobs across all queues |
| `GET` | `/api/queues/outbox/stats` | Outbox counts by status, oldest pending age, last failure |
| `POST` | `/api/queues/outbox/requeue-failed` | Move `FAILED` outbox rows back to `PENDING` (optional `{ "eventIds": [...] }`) |

### Ledger Service (Spring Boot)

| Method | Endpoint | Port | Description |
|--------|----------|------|-------------|
| `GET` | `/api/ledger/orders/{orderId}` | 8090 → 8080 | Ledger entry + lines for one order (`404` until consumed) |
| `GET` | `/api/ledger/users/{userId}?page=0&size=20` | 8090 → 8080 | Paginated ledger for a user, newest first (`size` ≤ 100) |
| `GET` | `/actuator/health` (`/liveness`, `/readiness`) | 8081 | Health probes; readiness includes the database |
| `GET` | `/actuator/prometheus` | 8081 | Metrics, including `ledger_events_*` counters |

### ML Service (Python — Internal)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check + model loaded status |
| `GET` | `/recommend/{user_id}` | Get top-10 SVD-predicted product IDs |
| `POST` | `/update-matrix` | Inject purchase events into interaction DataFrame |
| `POST` | `/retrain` | Manual override: force SVD model rebuild |

---

<p align="center">
  <strong>Built with ❤️ by Tushank </strong><br/>
  <em>Every architectural decision in this codebase exists to solve a real problem.</em>
</p>
