

<h1 align="center">Distributed Event-Driven Recommendation Platform</h1>

<p align="center">
  <em>
    A production-grade <strong>distributed recommendation platform</strong> built around an
<strong>event-driven microservices architecture</strong>.<br/><br/>

The system decouples <strong>customer-facing APIs</strong> from computationally intensive
<strong>SVD-based recommendation pipelines</strong> using asynchronous processing,
enabling <strong>scalable, low-latency product services</strong> alongside
<strong>personalized ML inference</strong>.
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
- [Phased Evolution Strategy](#phased-evolution-strategy)
- [System Architecture](#system-architecture)
- [Core System Capabilities](#core-system-capabilities)
- [Technical Deep Dives — The Debug Chronicles](#technical-deep-dives--the-debug-chronicles)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Environment Variables](#environment-variables)
- [API Reference](#api-reference)
- [Future Scope & Scale Horizons](#future-scope--scale-horizons)

---

## The Engineering Problem

Every major e-commerce platform faces the same fundamental systems design challenge: the **storefront** demands instant page loads with reliable database reads, while the **recommendation engine** needs to run computationally intensive matrix algebra — extracting latent user preference vectors from sparse rating matrices via SVD decomposition.

If both workloads share the same execution thread and the same database connection pool, a single heavy recommendation recalculation can spike CPU for hundreds of milliseconds and block every other user's product page from loading.

This project solves that tension by **decoupling** the two concerns into separate service boundaries connected via an asynchronous event bus, dedicated caching layers, and isolated connection pools — the same architectural pattern used at companies like Amazon and Netfilx to run ML inference alongside transactional APIs.

---

## Phased Evolution Strategy

This platform was not built in a single pass. It was engineered iteratively across six deliberate optimization phases, each targeting a specific bottleneck:

### Phase 1 → The Gateway Pattern (API Orchestration & BFF)

**Goal:** Abstract all internal services behind a single Express.js Gateway, improving security, simplifying frontend integration, and reducing network round-trips.

**What was implemented:**
- A Node.js Express server acts as the Backend-for-Frontend (BFF) layer, exposing a unified `/api/*` namespace to the React frontend
- All internal routing — users, products, categories, orders, uploads — is consolidated into one entry point
- JWT-based authentication via HTTP-only cookies is enforced at the gateway layer through `authMiddleware.js` before requests ever reach controllers
- CORS policies whitelist specific frontend origins and allow credentialed cross-origin requests


---

### Phase 2 → Database Persistence Optimization (MongoDB Indexing)

**Goal:** Move MongoDB from `O(n)` full collection scans to `O(log n)` index-based lookups, reducing database CPU consumption on every read.

**What was implemented:**

The `productModel.js` schema defines three targeted indexes that the MongoDB query planner now uses instead of scanning every document:

| Index | Fields | Purpose |
|-------|--------|---------|
| **Compound B-Tree** | `{ category: 1, price: 1 }` | Enables efficient category + price range filtering without collection scans |
| **Sort Index** | `{ createdAt: -1 }` | Eliminates in-memory sort overhead on "newest first" product listings |
| **Weighted Text Index** | `{ name: "text", description: "text", brand: "text" }` with weights `{ name: 10, brand: 5, description: 1 }` | Powers native `$text` full-text search with relevance scoring — product name matches are ranked 10× higher than description matches |

**The optimization pivot:** The `fetchProducts` controller originally used `$regex` for keyword searches, which cannot use indexes and forces a full collection scan on every keystroke. This was replaced with MongoDB's native `$text` operator, which offloads tokenization, stemming, and text filtering directly to Atlas cluster memory.

An explicit `/api/products/explain-query` diagnostic endpoint was built to expose MongoDB's `executionStats` query planner output, verifying that the `IXSCAN` (index scan) stage is used instead of `COLLSCAN` (collection scan).

---

### Phase 3 → The Speed Layer (Distributed Redis Caching)

**Goal:** Bring read latencies down to sub-10ms for frequently accessed, computationally expensive recommendation datasets.

**What was implemented:**
- An `ioredis` client connects to Redis (local in development, Upstash via `rediss://` TLS in production) as a dedicated in-memory cache layer
- Personalized recommendation results are stored under `recs:{userId}` keys with a 24-hour TTL (`EX 86400`)
- On cache hit, the recommendation endpoint returns instantly from Redis RAM without touching MongoDB or the Python ML sidecar
- On cache miss, the system queries the Python SVD service, fetches matching product documents from MongoDB, and seeds the cache for subsequent requests
- A **Redis Mutex Lock** pattern (`SET key "locked" EX 5 NX`) prevents cache stampede — when the cache expires, only the first concurrent request acquires the lock and performs the expensive recalculation; subsequent requests are served fallback data instead of all racing to rebuild the cache simultaneously
- A dedicated `benchmark/cache-stampede-simulation.js` script fires 10 simultaneous requests to validate the stampede prevention mechanism

---

### Phase 4 → Decoupling via Event-Driven Architecture (EDA)

**Goal:** Move slow, write-heavy, and computation-intensive processes out of the critical user request-response path.

**What was implemented:**

Two BullMQ queues backed by Redis handle asynchronous downstream processing:

| Queue | Trigger | What the Worker Does |
|-------|---------|---------------------|
| `order-events` | A new order is placed | 1. Invalidates the user's cached recommendations (`redis.del(recs:{userId})`). 2. Sends a `POST /update-matrix` payload to the Python ML sidecar with purchased product IDs so the interaction matrix can be updated with implicit purchase signals (rating weight: 5.0). |
| `recommendation-updates` | A product review is submitted | 1. Calls `GET /recommend/{userId}` on the Python SVD service. 2. Fetches the returned product IDs from MongoDB. 3. Writes the ordered recommendation array into Redis, hot-swapping the user's cached results. |

Both queues are configured with exponential backoff retry policies (`attempts: 3`, `backoff: exponential/2000ms`) and `concurrency: 5` workers for parallel job processing. Workers use isolated Redis connections (via `createRedisConnection()`) to prevent socket collisions with the main cache client — the critical fix documented in the [TLS Socket Collision Case Study](#-technical-case-study-resolving-asynchronous-connection-hijacking--socket-collision-over-cloud-tls) below.

The key design principle: **the shopper's checkout response (`201 Created`) is returned instantly after the order is persisted to MongoDB.** All downstream analytics — cache invalidation, ML matrix updates, recommendation recalculation — happen asynchronously via the BullMQ worker pool without blocking the client's HTTP thread.

---

### Phase 5 → The Machine Learning Recommendation Core

**Goal:** Build a recommendation pipeline that mirrors real-world production architectures — a separate Python microservice running SVD matrix factorization on user-product rating data.

**What was implemented:**

A FastAPI Python service (`ml-backend/main.py`) runs as an independent sidecar process:

**Training Pipeline:**
1. On startup, connects to MongoDB and executes a server-side aggregation pipeline that `$unwind`s the embedded `reviews` array across all products, projecting flat `{user, item, rating}` tuples
2. Loads the flattened rating matrix into a Pandas DataFrame
3. Trains a **Singular Value Decomposition (SVD)** model from the `surprise` library — projecting the sparse user-product interaction matrix into low-dimensional latent factor spaces where similar users and items cluster together
4. Stores the trained model in memory (`GLOBAL_MODEL`) for instant inference

**Cold-Start Gate:**
A critical safeguard prevents the SVD engine from crashing on sparse data. If `len(ratings_data) < 5`, the model training is completely skipped to avoid null matrix decomposition errors. On the Node.js side, the recommendation controller independently checks if the user has any review history (`Product.findOne({ "reviews.user": userId })`). Users with zero interaction history are served a top-rated product fallback instead of empty recommendations.

**Inference:**
The `GET /recommend/{userId}` endpoint iterates over all product IDs, calls `model.predict(user_id, item_id)` to estimate the user's rating for each product, sorts by predicted score descending, and returns the top 10 product IDs.

**Live Matrix Updates:**
The `POST /update-matrix` endpoint accepts streaming purchase event payloads from the Node.js BullMQ order worker. Purchase events are injected into the in-memory DataFrame as high-confidence implicit ratings (score: 5.0), making them available for the next retraining cycle without requiring a full MongoDB re-extraction.

---

### Phase 6 → Production MLOps, Pipelines & Monitoring

**Goal:** Automate model lifecycle management with scheduled retraining — the same operational discipline professional ML engineers use in production.

**What was implemented:**

An **APScheduler** `BackgroundScheduler` daemon runs natively inside the FastAPI process:

| Schedule | Rule | Behavior |
|----------|------|----------|
| **Production** | `cron` — daily at 3:00 AM | Full SVD matrix refactorization during off-peak hours, consuming the latest DataFrame (including all injected purchase/review events since the last training run) |
| **Demo/Staging** | `interval` — every 2 minutes | Rapid retraining cycle for live demonstration and verification of the auto-retrain pipeline |

A manual override is also exposed via `POST /retrain` for on-demand model rebuilds during development and presentation.

> ** Note on diagrams:** The architecture diagrams below show the *target vision* for this platform's evolution. Components such as Two-Tower Neural Networks, MLflow Model Registry, and Vector DB retrieval are part of the planned [Future Scope](#future-scope--scale-horizons) and are **not yet implemented** in the current codebase. Everything documented in the phases above reflects actual working code.

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
| **Future Offline** | `Two-Tower Deep Model → Model Evaluation (Recall@K, NDCG) → MLflow Model Registry` |  Planned — see [Future Scope](#future-scope--scale-horizons) |

---

## Core System Capabilities

###  The Systems Engineering Layer

<table>
<tr>
<th width="30%">Capability</th>
<th width="70%">Implementation Detail</th>
</tr>
<tr>
<td><strong>Backend-for-Frontend (BFF) Gateway</strong></td>
<td>Node.js Express server acts as the single API surface. The React frontend only communicates with <code>/api/*</code> — it has zero knowledge of MongoDB, Redis, or the Python service. Internal service topology can be refactored without any frontend changes.</td>
</tr>
<tr>
<td><strong>Event-Driven Asynchrony</strong></td>
<td>BullMQ queues process checkout and review events instantly without blocking client HTTP threads. The order controller persists the order to MongoDB, drops a job into the Redis-backed queue, and returns <code>201 Created</code> — total client-facing latency is dominated only by the MongoDB write, not by downstream ML or cache operations.</td>
</tr>
<tr>
<td><strong>Cache Stampede Prevention</strong></td>
<td>Redis <code>SET NX</code> mutex locks ensure that when a hot cache key expires under high concurrency, only one request rebuilds the cache. Other concurrent requests receive a graceful fallback (top-rated products) instead of all hammering the Python sidecar simultaneously.</td>
</tr>
<tr>
<td><strong>Connection Isolation</strong></td>
<td>The Redis module exports a <code>createRedisConnection()</code> factory — each BullMQ worker daemon gets its own dedicated TCP socket, preventing the TLS socket hijacking crisis documented below.</td>
</tr>
<tr>
<td><strong>Optimized Database Layer</strong></td>
<td>Three MongoDB indexes (compound B-Tree, sort, weighted text) bring query patterns from <code>COLLSCAN</code> to <code>IXSCAN</code> — verified via the built-in <code>/explain-query</code> diagnostic endpoint.</td>
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
<td>Purchase events streamed via BullMQ are injected into the in-memory Pandas DataFrame as implicit high-confidence ratings (score 5.0). This ensures that the next scheduled retraining cycle incorporates recent purchase behavior without requiring a full MongoDB re-extraction.</td>
</tr>
<tr>
<td><strong>Automated Batch Retraining</strong></td>
<td>APScheduler runs two concurrent schedules: a production cron job (daily at 3 AM) and a demo interval (every 2 minutes). Both trigger a full SVD <code>.fit()</code> on the current DataFrame, atomically swapping the global model reference.</td>
</tr>
<tr>
<td><strong>Prediction Serving</strong></td>
<td>For a given user, the model predicts estimated ratings for every product in the catalog, sorts by predicted score, and returns the top 10 IDs. The Node.js gateway then fetches the full product documents from MongoDB, preserving the SVD-predicted ranking order.</td>
</tr>
</table>

---

## Technical Deep Dives — The Debug Chronicles

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

By changing the instantiation inside `orderQueue.js` and `recommendationQueue.js` to ingest the **raw config factory** instead of the client pointer, each BullMQ worker daemon operates on its own dedicated TCP socket:

```javascript
// orderQueue.js — Isolated Connection Per Worker

export const orderQueue = new Queue("order-events", {
  connection: createRedisConnection(),  // ← Dedicated socket
});

const orderWorker = new Worker("order-events", async (job) => {
  // ... async processing
}, {
  connection: createRedisConnection(),  // ← Another dedicated socket
  concurrency: 5,
});
```

**Result:** Three isolated Redis connections now run in parallel — one for the main API cache, one for each BullMQ queue, and one for each worker — eliminating all cross-talk on the TLS channel. The 502 failures resolved immediately and the system maintained zero-downtime stability under high-concurrency load.

---

## Project Structure

```
                 

├── backend/
│   ├── config/
│   │   ├── db.js                 # MongoDB connection with reconnection guard
│   │   └── redis.js              # Redis factory: config, createRedisConnection(), singleton client
│   ├── controllers/
│   │   ├── productController.js  # CRUD + text search + recommendations + mutex lock + explain query
│   │   ├── orderController.js    # Order creation with async BullMQ dispatch
│   │   ├── userController.js     # Auth + cache eviction on logout
│   │   └── categoryController.js # Category CRUD
│   ├── middlewares/
│   │   ├── authMiddleware.js     # JWT cookie verification + admin role gate
│   │   ├── asyncHandler.js       # Express async error wrapper
│   │   ├── checkId.js            # MongoDB ObjectId format validator
│   │   └── errorMiddleware.js    # Global 404 & error response formatting
│   ├── models/
│   │   ├── productModel.js       # Product schema + review subdoc + 3 MongoDB indexes
│   │   ├── orderModel.js         # Order schema with payment result tracking
│   │   ├── userModel.js          # User schema with admin flag
│   │   └── categoryModel.js      # Category schema
│   ├── queues/
│   │   ├── orderQueue.js         # BullMQ queue + worker: purchase → ML matrix update
│   │   └── recommendationQueue.js # BullMQ queue + worker: review → cache hot-swap
│   ├── routes/
│   │   ├── productRoutes.js      # Product API routes including /recommendations
│   │   ├── orderRoutes.js        # Order API routes with admin-only listing
│   │   ├── userRoutes.js         # User auth & profile routes
│   │   ├── categoryRoutes.js     # Category admin routes
│   │   └── uploadRoutes.js       # Cloudinary / local disk dual-mode image uploads
│   ├── scripts/
│   │   ├── fixProductImages.js   # Bulk image URL migration utility
│   │   └── migrateImages.js      # Cloudinary migration script
│   ├── utils/
│   │   └── createToken.js        # JWT token generator + HTTP-only cookie setter
│   ├── seeder.js                 # Database seeder: 50+ products, 20 users, review generation
│   └── index.js                  # Express app: CORS, routes, health checks, static serving
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
│   └── cache-stampede-simulation.js  # 10-concurrent-request stampede test
│
├── docs/
│   ├── system-architecture-flow.png  # End-to-end system flow diagram
│   └── offline-online-pipeline.png   # Offline vs online pipeline diagram
│
├── package.json                  # Monorepo root: Node.js dependencies + workspace scripts
├── pnpm-workspace.yaml           # PNPM workspace: backend + frontend packages
                   
```

---

## Getting Started

### Prerequisites

- **Node.js** ≥ 18
- **pnpm** (or npm/yarn)
- **Python** ≥ 3.9
- **MongoDB** (Atlas or local)
- **Redis** (local or Upstash cloud)

### 1. Clone & Install

```bash
git clone https://github.com/tushank99/Shopify-app.git
cd Shopify-app

# Install Node.js dependencies (monorepo root + frontend)
pnpm install
cd frontend && npm install && cd ..
```

### 2. Set Up the Python ML Service

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

### 3. Configure Environment Variables

Create a `.env` file in the project root (see [Environment Variables](#environment-variables) below).

### 4. Seed the Database (Optional)

```bash
cd backend
node seeder.js
```

This seeds 50+ products across 5 categories with 20 test users and realistic review distributions — providing enough rating data for the SVD model to train meaningfully.

### 5. Start All Services

Open three terminal windows:

```bash
# Terminal 1 — Node.js Backend
pnpm run backend

# Terminal 2 — React Frontend
pnpm run frontend

# Terminal 3 — Python ML Service
pnpm run ml
```

| Service | URL |
|---------|-----|
| React Frontend | `http://localhost:5173` |
| Node.js API Gateway | `http://localhost:5000` |
| Python ML Service | `http://localhost:8000` |
| ML Health Check | `http://localhost:8000/health` |

---

## Environment Variables

Create a `.env` file in the project root:

```env
# Server
PORT=5000
NODE_ENV=development

# MongoDB
MONGO_URI=mongodb+srv://<username>:<password>@cluster.mongodb.net/<dbname>

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

# Redis — Production (Upstash)
# REDIS_URL=rediss://default:password@endpoint.upstash.io:6379

# Python ML Service URL (for BullMQ workers)
# ML_SERVICE_URL=http://127.0.0.1:8000

# Frontend URL (for CORS)
# FRONTEND_URL=https://your-frontend.vercel.app
```

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
| `POST` | `/api/orders` | Private | Create order → triggers async BullMQ order event |
| `GET` | `/api/orders/mine` | Private | Get current user's orders |
| `GET` | `/api/orders/:id` | Private | Get order by ID |
| `GET` | `/api/orders` | Admin | List all orders |

### ML Service (Python — Internal)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check + model loaded status |
| `GET` | `/recommend/{user_id}` | Get top-10 SVD-predicted product IDs |
| `POST` | `/update-matrix` | Inject purchase events into interaction DataFrame |
| `POST` | `/retrain` | Manual override: force SVD model rebuild |

---

## Future Scope & Scale Horizons

These are the next engineering milestones planned for this platform — they represent genuine architectural evolutions, not features that currently exist in the codebase:

### 1. Real-Time Streaming Pipeline
Replace the background APScheduler cron daemon with a **streaming data broker** like Apache Kafka or AWS Kinesis to feed real-time user clickstream events (page views, add-to-cart, wishlist interactions) directly into the ML sidecar. This would transition the system from periodic batch retraining to continuous online learning.

### 2. Advanced Embedding Retrievals
Migrate from standard SVD matrix arrays to an explicit **Two-Tower Neural Network** architecture or a dedicated **Vector Database** (like Milvus or Pinecone) for approximate nearest neighbor (ANN) lookups. As the catalog scales to millions of items, iterating over every product ID for predictions becomes infeasible — ANN indexes enable sub-millisecond retrieval over billion-scale embedding spaces.

### 3. Rigorous MLOps Tracking
Integrate an **MLflow Model Registry** to track model training metrics (Precision@K, Recall@K, NDCG) continuously against production deployment baselines. Each retraining cycle would produce a versioned model artifact with associated evaluation metrics, enabling automated rollback if a new model degrades recommendation quality.

### 4. A/B Testing Framework
Implement a feature-flagged recommendation serving layer that can route a percentage of traffic to a challenger model while the champion model serves the majority. This allows quantitative comparison of different algorithms (e.g., SVD vs. neural collaborative filtering) on live user engagement metrics.

### 5. Content-Based Hybrid Filtering
Augment the current collaborative filtering (SVD) with content-based signals — product descriptions encoded via TF-IDF or sentence transformers. A hybrid approach handles the cold-start problem more gracefully for new products that have zero reviews but rich text descriptions.

---

<p align="center">
  <strong>Built with ❤️ by Tushank </strong><br/>
  <em>Every architectural decision in this codebase exists to solve a real problem.</em>
</p>
