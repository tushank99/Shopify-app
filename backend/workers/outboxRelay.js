/**
 * Standalone outbox relay. Run:  node backend/workers/outboxRelay.js   (pnpm run relay)
 *
 * - Not imported by backend/index.js: it connects to Mongo itself, handles SIGTERM and exposes /healthz.
 * - Delivery is AT-LEAST-ONCE. KafkaJS idempotence only protects within one producer session, so the
 *   real guarantee is consumer-side idempotency on eventId (ledger_entry.event_id).
 * - Multiple instances are safe: rows are claimed atomically with a lease + fencing token.
 */
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import mongoose from "mongoose";
import { Kafka, Partitioners, logLevel } from "kafkajs";
import { context, propagation, trace, SpanStatusCode } from "@opentelemetry/api";

import Outbox from "../models/outboxModel.js";
import {
  claimNextRow,
  markPublished,
  markFailedAttempt,
  releaseForConnectivity,
  getOutboxStats,
} from "../outbox/outboxStore.js";
import {
  classifyError,
  needsProducerRecycle,
  SendTimeoutError,
  PayloadSerializationError,
} from "../outbox/kafkaErrors.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, "../../.env") }); // same .env as backend/index.js

const intEnv = (name, def) => {
  const v = parseInt(process.env[name], 10);
  return Number.isFinite(v) && v > 0 ? v : def;
};

const cfg = {
  mongoUri: process.env.MONGO_URI,
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(",").map((s) => s.trim()).filter(Boolean),
  clientId: process.env.KAFKA_CLIENT_ID || "shopify-outbox-relay",
  leaseMs: intEnv("OUTBOX_LEASE_MS", 30_000),
  sendTimeoutMs: intEnv("OUTBOX_SEND_TIMEOUT_MS", 8_000),
  batchSize: intEnv("OUTBOX_BATCH_SIZE", 100),
  idleSleepMs: intEnv("OUTBOX_IDLE_SLEEP_MS", 300),
  maxAttempts: intEnv("OUTBOX_MAX_ATTEMPTS", 5),
  gateMaxBackoffMs: intEnv("OUTBOX_GATE_MAX_BACKOFF_MS", 30_000),
  healthPort: intEnv("OUTBOX_HEALTH_PORT", 8085),
  statsIntervalMs: intEnv("OUTBOX_STATS_INTERVAL_MS", 30_000),
};

// The Kafka send timeout must stay far below the lease, otherwise a slow-but-alive send can outlive
// its lease and a second relay would publish the same row while this one is still sending.
if (cfg.sendTimeoutMs * 3 > cfg.leaseMs) {
  console.error(
    `[outbox] invalid config: OUTBOX_SEND_TIMEOUT_MS (${cfg.sendTimeoutMs}) must be <= OUTBOX_LEASE_MS/3 (${cfg.leaseMs}/3)`
  );
  process.exit(1);
}
if (!cfg.mongoUri) {
  console.error("[outbox] MONGO_URI is not set");
  process.exit(1);
}

const log = (level, msg, extra) =>
  console[level === "error" ? "error" : "log"](`[outbox] ${msg}`, extra !== undefined ? extra : "");

// ----------------------------------------------------------------------------------------------
// Kafka
// ----------------------------------------------------------------------------------------------
const kafka = new Kafka({
  clientId: cfg.clientId,
  brokers: cfg.brokers,
  logLevel: logLevel.WARN,
  connectionTimeout: 3_000,
  requestTimeout: cfg.sendTimeoutMs,
  retry: { initialRetryTime: 200, retries: 3 }, // bounded: the gate (below) owns long outages
});

const makeProducer = () =>
  kafka.producer({
    allowAutoTopicCreation: false,
    idempotent: true, // experimental in KafkaJS + per-session only; consumer idempotency is the real guard
    maxInFlightRequests: 1,
    createPartitioner: Partitioners.DefaultPartitioner, // murmur2 == Java client => stable key->partition
  });

let producer = makeProducer();
let producerConnected = false;
const admin = kafka.admin({ retry: { initialRetryTime: 200, retries: 1 } });
let adminConnected = false;

const withTimeout = (promise, ms) => {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new SendTimeoutError(ms)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
};

async function ensureProducer() {
  if (!producerConnected) {
    await withTimeout(producer.connect(), cfg.sendTimeoutMs);
    producerConnected = true;
  }
}

async function recycleProducer() {
  try {
    await producer.disconnect();
  } catch {
    /* already dead */
  }
  producerConnected = false;
  producer = makeProducer();
}

async function brokerReachable(topic) {
  try {
    if (!adminConnected) {
      await withTimeout(admin.connect(), 4_000);
      adminConnected = true;
    }
    const meta = await withTimeout(admin.fetchTopicMetadata({ topics: [topic] }), 4_000);
    return meta.topics.length > 0;
  } catch (err) {
    try {
      await admin.disconnect();
    } catch {
      /* ignore */
    }
    adminConnected = false;
    return false;
  }
}

// ----------------------------------------------------------------------------------------------
// Broker gate: while the broker is unreachable we stop claiming rows at all.
// ----------------------------------------------------------------------------------------------
const gate = { open: true, failures: 0, probeTopic: null, recycle: false };
const state = {
  startedAt: Date.now(),
  lastLoopAt: Date.now(),
  published: 0,
  releasedForConnectivity: 0,
  failedRows: 0,
  retriedRows: 0,
  lostLeases: 0,
  loopErrors: 0,
};
let shuttingDown = false;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// Interruptible sleep so SIGTERM doesn't wait out a long gate backoff.
const sleepUnlessStopping = async (ms) => {
  const end = Date.now() + ms;
  while (!shuttingDown && Date.now() < end) await sleep(Math.min(250, end - Date.now()));
};

function closeGate(err, topic) {
  gate.open = false;
  gate.failures += 1;
  gate.probeTopic = topic;
  gate.recycle = gate.recycle || needsProducerRecycle(err);
  log("error", `broker unreachable (${err?.message}); pausing claims (failure #${gate.failures})`);
}

async function waitForBroker() {
  while (!shuttingDown && !gate.open) {
    const wait = Math.min(cfg.gateMaxBackoffMs, 500 * 2 ** Math.min(gate.failures, 8));
    await sleepUnlessStopping(wait);
    if (shuttingDown) return;
    if (await brokerReachable(gate.probeTopic)) {
      if (gate.recycle) {
        await recycleProducer();
        gate.recycle = false;
      }
      try {
        await ensureProducer();
        gate.open = true;
        gate.failures = 0;
        log("log", "broker reachable again; resuming claims");
      } catch (err) {
        gate.failures += 1;
        log("error", `broker answered metadata but producer connect failed: ${err.message}`);
      }
    } else {
      gate.failures += 1;
    }
  }
}

// ----------------------------------------------------------------------------------------------
// Publishing
// ----------------------------------------------------------------------------------------------
const tracer = trace.getTracer("outbox-relay");

function buildMessage(row) {
  let value;
  try {
    value = JSON.stringify(row.payload);
    if (value === undefined) throw new Error("payload is undefined");
  } catch (err) {
    throw new PayloadSerializationError(err);
  }
  const headers = {
    "event-id": row.eventId,
    "content-type": "application/json",
  };
  // Continue the trace of the originating HTTP request (no-op without an OTel SDK).
  propagation.inject(context.active(), headers);
  return { key: String(row.partitionKey), value, headers };
}

async function publish(row) {
  // Restore the trace context captured in createOrder BEFORE producing.
  const parentCtx = propagation.extract(context.active(), row.traceCarrier || {});
  return context.with(parentCtx, () =>
    tracer.startActiveSpan(`outbox publish ${row.topic}`, async (span) => {
      span.setAttribute("outbox.event_id", row.eventId);
      span.setAttribute("messaging.destination.name", row.topic);
      try {
        const message = buildMessage(row);
        await ensureProducer();
        await withTimeout(
          producer.send({ topic: row.topic, acks: -1, timeout: cfg.sendTimeoutMs, messages: [message] }),
          cfg.sendTimeoutMs
        );
      } catch (err) {
        span.recordException(err);
        span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
        throw err;
      } finally {
        span.end();
      }
    })
  );
}

/** @returns "published" | "connectivity" | "retry" | "failed" | "lost" */
async function processRow(row) {
  let sendError = null;
  try {
    await publish(row);
  } catch (err) {
    sendError = err;
  }

  // Failure-drill hook (drill 7): die AFTER Kafka acknowledged the message but BEFORE the row is marked
    // PUBLISHED. Only active when OUTBOX_CHAOS_KILL_AFTER_SEND=1; never set in production.
  if (!sendError && process.env.OUTBOX_CHAOS_KILL_AFTER_SEND === "1") {
    log("error", `[CHAOS] SIGKILL after send of ${row.eventId}, before markPublished`);
    process.kill(process.pid, "SIGKILL");
  }
  if (!sendError) {
    const ok = await markPublished(row);
    if (!ok) {
      // We sent it but lost the lease meanwhile. Kafka may now hold a duplicate; the consumer dedups on eventId.
      state.lostLeases += 1;
      log("error", `lost lease after send for ${row.eventId}; duplicate possible (consumer is idempotent)`);
      return "lost";
    }
    state.published += 1;
    return "published";
  }

  const kind = classifyError(sendError);

  if (kind === "connectivity") {
    await releaseForConnectivity(row, { error: sendError, retryInMs: 1000 });
    state.releasedForConnectivity += 1;
    closeGate(sendError, row.topic);
    return "connectivity";
  }

  const { updated, status } = await markFailedAttempt(row, {
    error: sendError,
    permanent: kind === "permanent",
    maxAttempts: cfg.maxAttempts,
  });
  if (!updated) {
    state.lostLeases += 1;
    return "lost";
  }
  if (status === "FAILED") {
    state.failedRows += 1;
    log("error", `[ALERT] outbox row ${row.eventId} -> FAILED (${kind}): ${sendError.message}`);
    return "failed";
  }
  state.retriedRows += 1;
  log("error", `publish failed for ${row.eventId} (${kind}); will retry: ${sendError.message}`);
  return "retry";
}

async function mainLoop() {
  while (!shuttingDown) {
    state.lastLoopAt = Date.now();
    try {
      if (!gate.open) {
        await waitForBroker();
        continue;
      }

      let processed = 0;
      while (processed < cfg.batchSize && !shuttingDown && gate.open) {
        const row = await claimNextRow({ leaseMs: cfg.leaseMs });
        if (!row) break;
        state.lastLoopAt = Date.now();
        await processRow(row);
        processed += 1;
      }

      // Drain continuously; sleep ONLY when the queue was empty.
      if (processed === 0 && gate.open) await sleepUnlessStopping(cfg.idleSleepMs);
    } catch (err) {
      // Mongo hiccup etc. A row that was sent but not marked stays PROCESSING and is reclaimed after the lease.
      state.loopErrors += 1;
      log("error", `loop error: ${err.message}`);
      await sleepUnlessStopping(1_000);
    }
  }
}

// ----------------------------------------------------------------------------------------------
// Observability: periodic stats + health endpoint
// ----------------------------------------------------------------------------------------------
async function logStats() {
  try {
    const s = await getOutboxStats();
    log(
      "log",
      `stats pending=${s.pending} processing=${s.processing} failed=${s.failed} oldestPendingAgeMs=${s.oldestPendingAgeMs} ` +
        `published=${state.published} gateOpen=${gate.open}`
    );
    if (s.failed > 0) {
      log(
        "error",
        `[ALERT] ${s.failed} outbox row(s) are FAILED. Last: ${s.lastFailed?.eventId} (${s.lastFailed?.lastError}). ` +
          "Fix the cause, then POST /api/queues/outbox/requeue-failed"
      );
    }
  } catch (err) {
    log("error", `stats failed: ${err.message}`);
  }
}

function startHealthServer() {
  const server = http.createServer((req, res) => {
    if (req.url === "/healthz" || req.url === "/livez") {
      // Liveness = the loop is making progress. A send can legitimately take up to sendTimeout.
      const stale = Date.now() - state.lastLoopAt > cfg.leaseMs;
      const mongoUp = mongoose.connection.readyState === 1;
      const healthy = !stale && mongoUp && !shuttingDown;
      res.writeHead(healthy ? 200 : 503, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          status: healthy ? "ok" : "unhealthy",
          mongoUp,
          brokerGateOpen: gate.open,
          lastLoopAgeMs: Date.now() - state.lastLoopAt,
          ...state,
        })
      );
      return;
    }
    res.writeHead(404).end();
  });
  server.listen(cfg.healthPort, "0.0.0.0", () => log("log", `health endpoint on :${cfg.healthPort}/healthz`));
  return server;
}

// ----------------------------------------------------------------------------------------------
// Lifecycle
// ----------------------------------------------------------------------------------------------
async function main() {
  await mongoose.connect(cfg.mongoUri, { serverSelectionTimeoutMS: 10_000 });
  await Outbox.init(); // build claim/TTL indexes before the first poll
  log("log", `mongo connected; brokers=${cfg.brokers.join(",")} lease=${cfg.leaseMs}ms sendTimeout=${cfg.sendTimeoutMs}ms`);

  const healthServer = startHealthServer();
  const statsTimer = setInterval(logStats, cfg.statsIntervalMs);
  statsTimer.unref();

  // Do not die if Kafka is down at boot: start with the gate closed and probe until it is up.
  try {
    await ensureProducer();
  } catch (err) {
    gate.probeTopic = "order.placed";
    closeGate(err, "order.placed");
  }

  const loop = mainLoop();

  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    log("log", `${signal} received; draining in-flight work`);
    const forced = setTimeout(() => {
      log("error", "graceful shutdown timed out; exiting (unmarked rows will be reclaimed after the lease)");
      process.exit(1);
    }, 15_000);
    forced.unref();
    try {
      await loop;
      clearInterval(statsTimer);
      healthServer.close();
      await producer.disconnect().catch(() => {});
      await admin.disconnect().catch(() => {});
      await mongoose.disconnect();
    } finally {
      process.exit(0);
    }
  };
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));

  await loop;
}

process.on("unhandledRejection", (reason) => log("error", `unhandledRejection: ${reason?.message || reason}`));

main().catch((err) => {
  log("error", `fatal: ${err.message}`);
  process.exit(1);
});