#!/usr/bin/env node
/**
 * Failure drills for the outbox -> Kafka -> ledger pipeline.
 *
 *   node scripts/drills/drills.mjs seed              # users + product + JWTs (benchmark/drill_data.json)
 *   node scripts/drills/drills.mjs all               # every drill, in order
 *   node scripts/drills/drills.mjs 1 3 6             # by number...
 *   node scripts/drills/drills.mjs volume replay     # ...or by key
 *
 * Prerequisites (the runner checks them):
 *   docker compose -f docker-compose.infra.yml --profile app up -d --build
 *   the Node backend running on BASE_URL (pnpm start); Redis reachable by it; k6 + docker on PATH
 *   NO relay running on :8085 -> the runner starts and kills relays itself (some drills need 2, or a chaos relay)
 *
 * Every check compares a delta against a baseline taken at the start of the drill, so clean volumes are not required.
 * Output: a table on stdout and benchmark/drill-results.md (paste it into the README).
 */
import { execFile, spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import mongoose from "mongoose";
import { Kafka, logLevel } from "kafkajs";
import Outbox from "../../backend/models/outboxModel.js";

const execFileAsync = promisify(execFile);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
dotenv.config({ path: path.join(ROOT, ".env") });

const COMPOSE_FILE = "docker-compose.infra.yml";
const BASE_URL = process.env.BASE_URL || "http://localhost:5000";
const BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",").map((s) => s.trim()).filter(Boolean);
const PG_USER = process.env.LEDGER_DB_USER || "ledger";
const GROUP = process.env.LEDGER_CONSUMER_GROUP || "ledger-service";
const MAIN_TOPIC = "order.placed";
const DLT_TOPIC = "order.placed-dlt";
const RETRY_TOPICS = [0, 1, 2, 3, 4].map((i) => `order.placed-retry-${i}`);
const RELAY_A = 8085;
const RELAY_B = 8086;
const RESULT_FILE = "benchmark/drill-result.json";
const VERBOSE = process.env.DRILL_VERBOSE === "1";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log("[drill]", ...a);

// ------------------------------------------------------------------------------------------------
// Shell / docker
// ------------------------------------------------------------------------------------------------
async function sh(cmd, args, opts = {}) {
  const { stdout } = await execFileAsync(cmd, args, {
    cwd: ROOT,
    maxBuffer: 64 * 1024 * 1024,
    windowsHide: true,
    ...opts,
  });
  return stdout;
}
const compose = (...args) => sh("docker", ["compose", "-f", COMPOSE_FILE, "--profile", "app", ...args]);

async function waitFor(label, probe, { timeoutMs = 120_000, everyMs = 1_000 } = {}) {
  const started = Date.now();
  let last = { ok: false, info: "" };
  for (;;) {
    try {
      last = await probe();
      if (last.ok) return last;
    } catch (err) {
      last = { ok: false, info: err.message.split("\n")[0] };
    }
    if (Date.now() - started > timeoutMs) throw new Error(`timeout waiting for ${label} (${last.info})`);
    await sleep(everyMs);
  }
}

// ------------------------------------------------------------------------------------------------
// Data sources: Postgres, Mongo outbox, Kafka, ledger metrics
// ------------------------------------------------------------------------------------------------
async function pgScalar(sql) {
  const out = await compose("exec", "-T", "postgres", "psql", "-U", PG_USER, "-d", "ledger_db", "-t", "-A", "-c", sql);
  const n = Number(out.trim());
  if (!Number.isFinite(n)) throw new Error(`psql returned "${out.trim()}"`);
  return n;
}
const ledgerCount = () => pgScalar("SELECT count(*) FROM ledger_entry");

async function outboxCounts() {
  const rows = await Outbox.aggregate([{ $group: { _id: "$status", n: { $sum: 1 } } }]);
  const c = { PENDING: 0, PROCESSING: 0, PUBLISHED: 0, FAILED: 0 };
  for (const r of rows) c[r._id] = r.n;
  return c;
}

const kafka = new Kafka({ clientId: "drill-runner", brokers: BROKERS, logLevel: logLevel.NOTHING });
const admin = kafka.admin();

async function offsets(topic) {
  const rows = await admin.fetchTopicOffsets(topic);
  return new Map(rows.map((r) => [r.partition, { low: Number(r.low), high: Number(r.high) }]));
}
async function topicTotal(topic) {
  let n = 0;
  for (const { low, high } of (await offsets(topic)).values()) n += high - low;
  return n;
}
async function retryTotal() {
  let n = 0;
  for (const t of RETRY_TOPICS) n += await topicTotal(t);
  return n;
}
async function consumerLag() {
  const end = await offsets(MAIN_TOPIC);
  const [res] = await admin.fetchOffsets({ groupId: GROUP, topics: [MAIN_TOPIC] });
  let lag = 0;
  for (const p of res.partitions) {
    const o = end.get(p.partition);
    const committed = Number(p.offset);
    lag += committed < 0 ? o.high - o.low : o.high - committed;
  }
  return lag;
}

async function snapshot() {
  const [ledger, outbox, dlt, retry] = await Promise.all([ledgerCount(), outboxCounts(), topicTotal(DLT_TOPIC), retryTotal()]);
  return { ledger, outbox, dlt, retry };
}

/** Sum of a Micrometer counter from the ledger's actuator (port 8081 is not published, so exec into the container). */
async function ledgerMetric(name, labelContains = "") {
  const url = "http://localhost:8081/actuator/prometheus";
  let text;
  try {
    text = await compose("exec", "-T", "ledger-service", "curl", "-sf", url);
  } catch {
    text = await compose("exec", "-T", "ledger-service", "wget", "-qO-", url);
  }
  let sum = 0;
  for (const line of text.split("\n")) {
    if (!line.startsWith(`${name}{`) && !line.startsWith(`${name} `)) continue;
    if (labelContains && !line.includes(labelContains)) continue;
    const v = Number(line.trim().split(/\s+/).pop());
    if (Number.isFinite(v)) sum += v;
  }
  return sum;
}
const DUP_ALL = "ledger_events_duplicate_total";
const DUP_EVENT_ID = ['ledger_events_duplicate_total', 'reason="event_id"'];

/** Reads [start, end) of a topic and returns the `event-id` header of every message. */
async function collectEventIds(topic, start, end) {
  const expected = [...end.entries()].reduce((s, [p, e]) => s + (e - start.get(p)), 0);
  if (expected <= 0) return [];
  const groupId = `drill-reader-${Date.now()}`;
  await admin.setOffsets({
    groupId,
    topic,
    partitions: [...start.entries()].map(([partition, offset]) => ({ partition, offset: String(offset) })),
  });
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });
  const ids = [];
  let done;
  const finished = new Promise((r) => (done = r));
  await consumer.run({
    eachMessage: async ({ message }) => {
      ids.push(message.headers?.["event-id"]?.toString() ?? "?");
      if (ids.length >= expected) done();
    },
  });
  await Promise.race([finished, sleep(60_000)]);
  await consumer.disconnect();
  return ids;
}

// ------------------------------------------------------------------------------------------------
// Relays (managed by the runner) and k6
// ------------------------------------------------------------------------------------------------
const relays = new Map(); // port -> child process

function startRelay(port, env = {}) {
  const child = spawn(process.execPath, ["backend/workers/outboxRelay.js"], {
    cwd: ROOT,
    env: { ...process.env, OUTBOX_HEALTH_PORT: String(port), ...env },
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  const echo = (d) => VERBOSE && process.stdout.write(`[relay:${port}] ${d}`);
  child.stdout.on("data", echo);
  child.stderr.on("data", echo);
  child.once("exit", () => relays.delete(port));
  relays.set(port, child);
  log(`relay started on :${port} (pid ${child.pid})`);
  return child;
}

async function stopRelays() {
  const children = [...relays.values()];
  for (const c of children) c.kill("SIGTERM");
  await Promise.race([Promise.all(children.map((c) => (c.exitCode !== null ? null : new Promise((r) => c.once("exit", r))))), sleep(20_000)]);
  for (const c of children) if (c.exitCode === null && c.signalCode === null) c.kill("SIGKILL");
  relays.clear();
}

async function relayState(port) {
  const res = await fetch(`http://127.0.0.1:${port}/healthz`, { signal: AbortSignal.timeout(3000) });
  return res.json();
}

async function runK6({ orders, users = 20, sleepS = 0.2 }) {
  const abs = path.join(ROOT, RESULT_FILE);
  fs.rmSync(abs, { force: true });
  const args = [
    "run", "-q",
    "-e", `ORDERS=${orders}`, "-e", `USERS=${users}`, "-e", `SLEEP_S=${sleepS}`,
    "-e", `BASE_URL=${BASE_URL}`, "-e", `RESULT_FILE=${RESULT_FILE}`,
    "benchmark/k6-drill-orders.js",
  ];
  // k6 exits 99 when a threshold fails; the result file is still written, and the drill checks decide pass/fail.
  await execFileAsync("k6", args, { cwd: ROOT, maxBuffer: 64 * 1024 * 1024, windowsHide: true }).catch((err) => {
    if (VERBOSE) console.log(String(err.stdout || err.message));
  });
  if (!fs.existsSync(abs)) throw new Error("k6 did not produce a result file (is k6 installed? did the seed run?)");
  return JSON.parse(fs.readFileSync(abs, "utf8"));
}

// ------------------------------------------------------------------------------------------------
// Preflight / settle / report helpers
// ------------------------------------------------------------------------------------------------
async function preflight() {
  if (!process.env.MONGO_URI) throw new Error("MONGO_URI is not set in .env");

  const running = (await compose("ps", "--status", "running", "--services")).split(/\r?\n/).map((s) => s.trim());
  for (const svc of ["mongo", "kafka", "postgres", "ledger-service"]) {
    if (!running.includes(svc)) {
      throw new Error(`compose service "${svc}" is not running (docker compose -f ${COMPOSE_FILE} --profile app up -d --build)`);
    }
  }

  const health = await fetch(`${BASE_URL}/api/health`, { signal: AbortSignal.timeout(3000) }).catch(() => null);
  if (!health?.ok) throw new Error(`backend not reachable at ${BASE_URL}/api/health (start it with: pnpm start)`);

  const external = await fetch(`http://127.0.0.1:${RELAY_A}/healthz`, { signal: AbortSignal.timeout(1500) }).catch(() => null);
  if (external) throw new Error(`a relay is already running on :${RELAY_A}; stop it, the drill runner manages relays itself`);

  await sh("k6", ["version"]).catch(() => {
    throw new Error("k6 not found on PATH");
  });

  if (!fs.existsSync(path.join(ROOT, "benchmark/drill_data.json"))) {
    log("benchmark/drill_data.json missing -> seeding");
    await runSeed();
  }
}

async function runSeed() {
  const out = await sh(process.execPath, ["benchmark/drill-seed.js"]);
  console.log(out.trim());
}

/** Start from a quiet system: drain old outbox rows (a stuck lease can take up to 30 s) and consumer lag. */
async function settle() {
  const c = await outboxCounts();
  if (c.PENDING + c.PROCESSING > 0) {
    log(`draining ${c.PENDING + c.PROCESSING} leftover outbox row(s)...`);
    startRelay(RELAY_A);
    await waitFor("outbox backlog to drain", async () => {
      const n = await outboxCounts();
      return { ok: n.PENDING + n.PROCESSING === 0, info: JSON.stringify(n) };
    }, { timeoutMs: 180_000 });
    await stopRelays();
  }
  await waitFor("consumer lag 0", async () => {
    const lag = await consumerLag();
    return { ok: lag === 0, info: `lag=${lag}` };
  }, { timeoutMs: 240_000, everyMs: 2_000 });
}

function check(d, label, actual, expected, { note = "" } = {}) {
  d.checks.push({ label, expected, actual, ok: actual === expected, note });
}

async function restoreInfra() {
  await compose("start", "kafka", "postgres", "ledger-service").catch(() => {});
}

// ------------------------------------------------------------------------------------------------
// The drills
// ------------------------------------------------------------------------------------------------

/** 1. Volume: 100 orders, reconciled three ways. */
async function volume(d) {
  const before = await snapshot();
  startRelay(RELAY_A);
  const k6 = await runK6({ orders: 100 });
  await waitFor("100 events PUBLISHED and ledgered", async () => {
    const s = await snapshot();
    const l = s.ledger - before.ledger;
    const p = s.outbox.PUBLISHED - before.outbox.PUBLISHED;
    return { ok: l >= 100 && p >= 100, info: `ledger +${l}, published +${p}` };
  });
  await sleep(3000); // would expose over-delivery
  const after = await snapshot();
  check(d, "k6: orders answered 201", k6.orders_created, 100);
  check(d, "Mongo outbox PUBLISHED (delta)", after.outbox.PUBLISHED - before.outbox.PUBLISHED, 100);
  check(d, "Postgres ledger_entry rows (delta)", after.ledger - before.ledger, 100);
  check(d, "Mongo outbox FAILED (delta)", after.outbox.FAILED - before.outbox.FAILED, 0);
  check(d, "DLT records (delta)", after.dlt - before.dlt, 0);
}

/** 2. Crash recovery: SIGKILL the Java app mid-stream, restart, everything drains exactly once. */
async function crashRecovery(d) {
  const before = await snapshot();
  startRelay(RELAY_A);
  const k6Promise = runK6({ orders: 100, sleepS: 1 }); // ~5 s of traffic: long enough to kill mid-way
  await waitFor("ledger to consume the first 10 rows", async () => {
    const l = (await ledgerCount()) - before.ledger;
    return { ok: l >= 10, info: `ledger +${l}` };
  }, { timeoutMs: 60_000, everyMs: 150 });

  const atKill = (await ledgerCount()) - before.ledger;
  log(`docker kill ledger-service with ${atKill}/100 rows ledgered`);
  await compose("kill", "ledger-service");
  const k6 = await k6Promise;
  await compose("start", "ledger-service");

  await waitFor("rebalance + drain to 100 rows", async () => {
    const l = (await ledgerCount()) - before.ledger;
    return { ok: l >= 100, info: `ledger +${l}` };
  }, { timeoutMs: 300_000, everyMs: 2_000 });
  await sleep(3000);
  const after = await snapshot();

  check(d, "app was killed before all 100 were ledgered (1 = yes)", atKill < 100 ? 1 : 0, 1, { note: `killed at ${atKill}` });
  check(d, "k6: orders answered 201", k6.orders_created, 100);
  check(d, "Mongo outbox PUBLISHED (delta)", after.outbox.PUBLISHED - before.outbox.PUBLISHED, 100);
  check(d, "Postgres ledger_entry rows (delta)", after.ledger - before.ledger, 100);
  check(d, "DLT records (delta)", after.dlt - before.dlt, 0);
}

/** 3. Duplicate replay: stop the consumer, shift committed offsets back by 10 PER PARTITION, restart. */
async function duplicateReplay(d) {
  const SHIFT = 10;
  const before = await snapshot();
  startRelay(RELAY_A);
  await runK6({ orders: 30 });
  await waitFor("30 rows ledgered", async () => {
    const l = (await ledgerCount()) - before.ledger;
    return { ok: l >= 30, info: `ledger +${l}` };
  });
  await waitFor("consumer lag 0", async () => ({ ok: (await consumerLag()) === 0, info: "" }), { everyMs: 1_000 });
  const ledgerBeforeReplay = await ledgerCount();

  // Offsets can only be reset while the consumer group is inactive.
  await compose("stop", "ledger-service");
  await waitFor("consumer group to become Empty", async () => {
    const { groups } = await admin.describeGroups([GROUP]);
    const state = groups[0]?.state;
    return { ok: state === "Empty" || state === "Dead", info: `state=${state}` };
  }, { timeoutMs: 90_000, everyMs: 2_000 });

  const bounds = await offsets(MAIN_TOPIC);
  const [committed] = await admin.fetchOffsets({ groupId: GROUP, topics: [MAIN_TOPIC] });
  const plan = [];
  let replayed = 0;
  for (const p of committed.partitions) {
    const current = Number(p.offset);
    if (current < 0) continue;
    const target = Math.max(bounds.get(p.partition).low, current - SHIFT);
    replayed += current - target;
    plan.push({ partition: p.partition, offset: String(target) });
  }
  await admin.setOffsets({ groupId: GROUP, topic: MAIN_TOPIC, partitions: plan });
  log(`offsets shifted back: ${replayed} record(s) will be redelivered (up to ${SHIFT} per partition)`);

  await compose("start", "ledger-service");
  // The JVM restarted, so its counters start at 0: duplicates counted == records replayed.
  await waitFor("replayed records to be consumed", async () => {
    const dup = await ledgerMetric(...DUP_EVENT_ID);
    return { ok: dup >= replayed, info: `duplicates=${dup}/${replayed}` };
  }, { timeoutMs: 180_000, everyMs: 2_000 });
  await sleep(3000);

  const dup = await ledgerMetric(...DUP_EVENT_ID);
  const ingested = await ledgerMetric("ledger_events_ingested_total");
  const after = await snapshot();
  check(d, "records replayed (informational, > 0 required)", replayed > 0 ? 1 : 0, 1, { note: `${replayed} replayed` });
  check(d, "'Duplicate eventId ignored' counter == records replayed", dup, replayed);
  check(d, "rows newly inserted during the replay", ingested, 0);
  check(d, "Postgres ledger_entry rows unchanged", after.ledger, ledgerBeforeReplay);
  check(d, "DLT records (delta)", after.dlt - before.dlt, 0);
}

/** 4. Poison pills: invalid bytes + valid JSON with an invalid schema; valid orders must keep flowing. */
async function poisonPills(d) {
  const before = await snapshot();
  startRelay(RELAY_A);

  const producer = kafka.producer({ allowAutoTopicCreation: false });
  await producer.connect();
  await producer.send({
    topic: MAIN_TOPIC,
    acks: -1,
    messages: [
      { key: "poison-invalid-json", value: Buffer.from('{"eventId":"550e8400-e29b-41d4-a716-446655440000","eventType":"order.placed","items":[{') },
      { key: "poison-invalid-schema", value: Buffer.from('{"corrupt":true}') },
    ],
  });
  await producer.disconnect();

  const k6 = await runK6({ orders: 10 });
  await waitFor("2 DLT records and 10 ledger rows", async () => {
    const s = await snapshot();
    return { ok: s.dlt - before.dlt >= 2 && s.ledger - before.ledger >= 10, info: `dlt +${s.dlt - before.dlt}, ledger +${s.ledger - before.ledger}` };
  }, { timeoutMs: 120_000 });
  await sleep(5000); // a retried pill would show up in the retry topics / DLT by now
  const after = await snapshot();

  check(d, "DLT records (delta): both poison pills, exactly once", after.dlt - before.dlt, 2);
  check(d, "retry-topic records (delta): pills skipped every retry", after.retry - before.retry, 0);
  check(d, "k6: valid orders answered 201", k6.orders_created, 10);
  check(d, "Postgres ledger_entry rows (delta): valid orders processed", after.ledger - before.ledger, 10);
}

/** 5. Broker outage: HTTP 201 keeps working, the outbox drains after recovery, nothing goes FAILED. */
async function brokerOutage(d) {
  const before = await snapshot();
  startRelay(RELAY_A);
  await sleep(3000);
  await compose("stop", "kafka");
  const k6 = await runK6({ orders: 20, users: 5 });
  await sleep(15_000); // let the relay hit the outage and close its gate
  const during = await snapshot();
  const gate = await relayState(RELAY_A);

  await compose("start", "kafka");
  await waitFor("outbox to drain and ledger to catch up", async () => {
    const s = await snapshot();
    const p = s.outbox.PUBLISHED - before.outbox.PUBLISHED;
    const l = s.ledger - before.ledger;
    return { ok: p >= 20 && l >= 20, info: `published +${p}, ledger +${l}` };
  }, { timeoutMs: 300_000, everyMs: 2_000 });
  await sleep(3000);
  const after = await snapshot();

  check(d, "k6: orders answered 201 during the outage", k6.orders_created, 20);
  check(d, "k6: orders rejected during the outage", k6.orders_rejected, 0);
  check(d, "outbox PUBLISHED during the outage (delta)", during.outbox.PUBLISHED - before.outbox.PUBLISHED, 0);
  check(d, "relay gate open during the outage", gate.brokerGateOpen, false);
  check(d, "outbox PUBLISHED after recovery (delta)", after.outbox.PUBLISHED - before.outbox.PUBLISHED, 20);
  check(d, "Postgres ledger_entry rows (delta)", after.ledger - before.ledger, 20);
  check(d, "outbox FAILED (delta)", after.outbox.FAILED - before.outbox.FAILED, 0);
}

/** 6. Two relays race for one backlog: no row is published twice, no DB errors. */
async function twoRelays(d) {
  const before = await snapshot();
  const dupBefore = await ledgerMetric(DUP_ALL);
  const k6 = await runK6({ orders: 100 }); // no relay running => 100 PENDING rows
  const topicStart = new Map([...(await offsets(MAIN_TOPIC))].map(([p, o]) => [p, o.high]));

  startRelay(RELAY_A);
  startRelay(RELAY_B);
  await waitFor("100 rows PUBLISHED and ledgered", async () => {
    const s = await snapshot();
    const p = s.outbox.PUBLISHED - before.outbox.PUBLISHED;
    const l = s.ledger - before.ledger;
    return { ok: p >= 100 && l >= 100, info: `published +${p}, ledger +${l}` };
  }, { timeoutMs: 180_000 });
  await sleep(3000);

  const after = await snapshot();
  const topicEnd = new Map([...(await offsets(MAIN_TOPIC))].map(([p, o]) => [p, o.high]));
  const ids = await collectEventIds(MAIN_TOPIC, topicStart, topicEnd);
  const [a, b] = await Promise.all([relayState(RELAY_A), relayState(RELAY_B)]);
  const dupAfter = await ledgerMetric(DUP_ALL);

  check(d, "k6: orders answered 201", k6.orders_created, 100);
  check(d, "messages produced to order.placed", ids.length, 100);
  check(d, "distinct event IDs on the topic", new Set(ids).size, 100);
  check(d, "rows published by relay A + relay B", a.published + b.published, 100, { note: `A=${a.published}, B=${b.published}` });
  check(d, "outbox FAILED (delta)", after.outbox.FAILED - before.outbox.FAILED, 0);
  check(d, "Postgres ledger_entry rows (delta)", after.ledger - before.ledger, 100);
  check(d, "ledger duplicate-ignored counter (delta): no DB constraint errors", dupAfter - dupBefore, 0);
  check(d, "DLT records (delta)", after.dlt - before.dlt, 0);
}

/** 7. Relay SIGKILLed between Kafka ack and markPublished: duplicate in Kafka, still one ledger row. */
async function relayCrash(d) {
  const N = 5;
  const lease = { OUTBOX_LEASE_MS: "9000", OUTBOX_SEND_TIMEOUT_MS: "3000" }; // short lease keeps the drill quick
  const before = await snapshot();
  const dupBefore = await ledgerMetric(...DUP_EVENT_ID);

  const k6 = await runK6({ orders: N, users: N }); // no relay running => N PENDING rows
  const topicStart = new Map([...(await offsets(MAIN_TOPIC))].map(([p, o]) => [p, o.high]));

  const chaos = startRelay(RELAY_A, { ...lease, OUTBOX_CHAOS_KILL_AFTER_SEND: "1" });
  await Promise.race([new Promise((r) => chaos.once("exit", r)), sleep(60_000)]);
  const crashed = chaos.exitCode !== 0 || chaos.signalCode !== null;
  const mid = await snapshot();

  startRelay(RELAY_B, lease); // reclaims the stuck row once its 9 s lease expires
  await waitFor("all rows PUBLISHED and ledgered", async () => {
    const s = await snapshot();
    const p = s.outbox.PUBLISHED - before.outbox.PUBLISHED;
    const l = s.ledger - before.ledger;
    return { ok: p >= N && l >= N, info: `published +${p}, ledger +${l}` };
  }, { timeoutMs: 120_000 });
  await sleep(3000);

  const after = await snapshot();
  const topicEnd = new Map([...(await offsets(MAIN_TOPIC))].map(([p, o]) => [p, o.high]));
  const ids = await collectEventIds(MAIN_TOPIC, topicStart, topicEnd);
  const dupAfter = await ledgerMetric(...DUP_EVENT_ID);

  check(d, "k6: orders answered 201", k6.orders_created, N);
  check(d, "relay A died abruptly (1 = yes)", crashed ? 1 : 0, 1);
  check(d, "rows PUBLISHED by the crashed relay (delta)", mid.outbox.PUBLISHED - before.outbox.PUBLISHED, 0);
  check(d, "rows stuck PROCESSING after the crash", mid.outbox.PROCESSING - before.outbox.PROCESSING, 1);
  check(d, "messages on order.placed: N orders + 1 duplicate", ids.length, N + 1);
  check(d, "distinct event IDs on the topic", new Set(ids).size, N);
  check(d, "Postgres ledger_entry rows (delta): still one per order", after.ledger - before.ledger, N);
  check(d, "'Duplicate eventId ignored' counter (delta)", dupAfter - dupBefore, 1);
  check(d, "outbox FAILED (delta)", after.outbox.FAILED - before.outbox.FAILED, 0);
  check(d, "DLT records (delta)", after.dlt - before.dlt, 0);
}

/** 8. Postgres outage SHORTER than the retry window (about 60 s): retries absorb it, nothing is dead-lettered. */
async function pgOutageShort(d) {
  await pgOutage(d, { outageSeconds: 20, expectDlt: false });
}

/** 8b. Postgres outage LONGER than the retry window: events reach the DLT, then the redrive restores them. */
async function pgOutageLong(d) {
  await pgOutage(d, { outageSeconds: 100, expectDlt: true });
}

async function pgOutage(d, { outageSeconds, expectDlt }) {
  const N = 10;
  const startedAt = Date.now();
  const before = await snapshot();
  startRelay(RELAY_A);
  await sleep(2000);

  await compose("stop", "postgres");
  const k6 = await runK6({ orders: N, users: 5 }); // the order path never touches Postgres
  log(`postgres stopped; waiting ${outageSeconds}s`);
  await sleep(outageSeconds * 1000);
  await compose("start", "postgres");
  await waitFor("postgres to accept queries", async () => ({ ok: (await ledgerCount()) >= 0, info: "" }), { timeoutMs: 90_000, everyMs: 2_000 });

  await waitFor("every event either ledgered or dead-lettered", async () => {
    const s = await snapshot();
    const l = s.ledger - before.ledger;
    const dl = s.dlt - before.dlt;
    return { ok: l + dl >= N, info: `ledger +${l}, dlt +${dl}` };
  }, { timeoutMs: 240_000, everyMs: 2_000 });

  const mid = await snapshot();
  check(d, "k6: orders answered 201 while Postgres was down", k6.orders_created, N);
  check(d, expectDlt ? "events dead-lettered after the retry window" : "events dead-lettered (retries absorbed the outage)",
    mid.dlt - before.dlt, expectDlt ? N : 0);

  if (mid.dlt - before.dlt > 0) {
    log("redriving the DLT...");
    console.log((await sh(process.execPath, ["scripts/dlt-redrive.mjs", "--since", String(startedAt)])).trim());
    await waitFor("redriven events ledgered", async () => {
      const l = (await ledgerCount()) - before.ledger;
      return { ok: l >= N, info: `ledger +${l}` };
    }, { timeoutMs: 180_000, everyMs: 2_000 });
  }
  await sleep(3000);
  const after = await snapshot();
  check(d, "Postgres ledger_entry rows after redrive (delta)", after.ledger - before.ledger, N);
  check(d, "outbox FAILED (delta)", after.outbox.FAILED - before.outbox.FAILED, 0);
}

// ------------------------------------------------------------------------------------------------
// Runner + report
// ------------------------------------------------------------------------------------------------
const DRILLS = [
  { id: "1", key: "volume", title: "Volume: 100 orders", run: volume },
  { id: "2", key: "crash", title: "Crash recovery: docker kill the Java app mid-stream", run: crashRecovery },
  { id: "3", key: "replay", title: "Duplicate replay: offsets shifted back 10 per partition", run: duplicateReplay },
  { id: "4", key: "poison", title: "Poison pills: invalid bytes + valid JSON/invalid schema", run: poisonPills },
  { id: "5", key: "broker-outage", title: "Broker outage: docker stop kafka, 20 orders", run: brokerOutage },
  { id: "6", key: "two-relays", title: "Relay concurrency: 2 relays, one backlog", run: twoRelays },
  { id: "7", key: "relay-crash", title: "Relay crash: kill -9 between send and mark", run: relayCrash },
  { id: "8", key: "pg-outage", title: "Postgres outage (20 s, inside the retry window)", run: pgOutageShort },
  { id: "8b", key: "pg-outage-long", title: "Postgres outage (100 s, beyond the retry window) + DLT redrive", run: pgOutageLong },
];

async function runDrill(def) {
  const d = { id: def.id, title: def.title, checks: [] };
  log(`=== Drill ${def.id}: ${def.title}`);
  try {
    await settle();
    await def.run(d);
  } catch (err) {
    d.checks.push({ label: "drill ran to completion", expected: "no error", actual: err.message, ok: false, note: "" });
  } finally {
    await stopRelays();
    await restoreInfra();
  }
  return d;
}

function report(results) {
  const lines = ["| Drill | Check | Expected | Actual | Result |", "|---|---|---|---|---|"];
  for (const d of results) {
    for (const c of d.checks) {
      const actual = c.note ? `${c.actual} (${c.note})` : String(c.actual);
      lines.push(`| ${d.id}. ${d.title} | ${c.label} | ${c.expected} | ${actual} | ${c.ok ? "PASS" : "**FAIL**"} |`);
    }
  }
  return lines.join("\n");
}

const args = process.argv.slice(2);
if (args.length === 0) {
  console.log("usage: node scripts/drills/drills.mjs seed | all | <drill id or key>...");
  console.log("drills: " + DRILLS.map((d) => `${d.id}=${d.key}`).join(", "));
  process.exit(0);
}

let exitCode = 0;
try {
  if (args[0] === "seed") {
    await runSeed();
  } else {
    const selected = args[0] === "all" ? DRILLS : args.map((a) => DRILLS.find((d) => d.id === a || d.key === a));
    if (selected.some((s) => !s)) throw new Error(`unknown drill in: ${args.join(" ")}`);

    await mongoose.connect(process.env.MONGO_URI, { serverSelectionTimeoutMS: 10_000 });
    await admin.connect();
    await preflight();

    const results = [];
    for (const def of selected) results.push(await runDrill(def));

    const table = report(results);
    console.log("\n" + table + "\n");
    fs.writeFileSync(path.join(ROOT, "benchmark/drill-results.md"), `${table}\n`);
    log("wrote benchmark/drill-results.md");
    if (results.some((r) => r.checks.some((c) => !c.ok))) exitCode = 1;
  }
} catch (err) {
  console.error(`[drill] fatal: ${err.message}`);
  exitCode = 1;
} finally {
  await stopRelays().catch(() => {});
  await admin.disconnect().catch(() => {});
  await mongoose.disconnect().catch(() => {});
}
process.exit(exitCode);