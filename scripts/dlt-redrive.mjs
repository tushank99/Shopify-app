/**
 * Replays dead-lettered events from order.placed-dlt back onto order.placed.
 * Safe to run more than once: the ledger de-duplicates on eventId, so a replayed event that was already
 * ledgered is counted as "Duplicate eventId ignored" and does not change any row.
 *
 *   node scripts/dlt-redrive.mjs [--dry-run] [--since <epoch-ms>] [--max <n>] [--include-invalid]
 *
 * By default records that are not structurally an order.placed event (invalid JSON, {"corrupt":true}, ...) are
 * SKIPPED: replaying a poison pill only sends it straight back to the DLT. --include-invalid replays everything.
 * Fix the root cause first (e.g. bring Postgres back) or the replay just dead-letters again.
 *
 * Original headers (event-id, traceparent, content-type) are kept; Spring's kafka_* DLT headers are dropped and
 * a `redriven-from` header (<dlt>:<partition>:<offset>) is added.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { Kafka, logLevel } from "kafkajs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, "../.env") });

const DLT_TOPIC = "order.placed-dlt";
const MAIN_TOPIC = "order.placed";
const BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",").map((s) => s.trim()).filter(Boolean);

const args = process.argv.slice(2);
const flag = (name) => args.includes(name);
const opt = (name) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : undefined;
};
const DRY_RUN = flag("--dry-run");
const INCLUDE_INVALID = flag("--include-invalid");
const SINCE = opt("--since") ? Number(opt("--since")) : 0;
const MAX = opt("--max") ? Number(opt("--max")) : Infinity;

const looksLikeOrderPlaced = (buf) => {
  if (!buf) return false;
  try {
    const e = JSON.parse(buf.toString("utf8"));
    return (
      e !== null &&
      typeof e === "object" &&
      typeof e.eventId === "string" &&
      typeof e.orderId === "string" &&
      typeof e.userId === "string" &&
      Array.isArray(e.items) &&
      e.items.length > 0
    );
  } catch {
    return false;
  }
};

const kafka = new Kafka({ clientId: "dlt-redrive", brokers: BROKERS, logLevel: logLevel.WARN });

// 1. Snapshot the DLT end offsets so we replay exactly what is there now.
const admin = kafka.admin();
await admin.connect();
const dltOffsets = await admin.fetchTopicOffsets(DLT_TOPIC);
await admin.disconnect();

const high = new Map(dltOffsets.map((o) => [o.partition, Number(o.high)]));
const target = new Map(dltOffsets.filter((o) => Number(o.high) > Number(o.low)).map((o) => [o.partition, Number(o.high)]));

if (target.size === 0) {
  console.log(`${DLT_TOPIC} is empty; nothing to do.`);
  process.exit(0);
}

// 2. Read the whole snapshot.
const consumer = kafka.consumer({ groupId: `dlt-redrive-${Date.now()}` });
await consumer.connect();
await consumer.subscribe({ topic: DLT_TOPIC, fromBeginning: true });

const collected = [];
const progress = new Map();
let markDone;
const finished = new Promise((resolve) => (markDone = resolve));

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    const offset = Number(message.offset);
    if (offset < high.get(partition)) collected.push({ partition, message });
    progress.set(partition, offset + 1);
    if ([...target.entries()].every(([p, end]) => (progress.get(p) ?? 0) >= end)) markDone();
  },
});
await Promise.race([finished, new Promise((r) => setTimeout(r, 60_000))]);
await consumer.disconnect();

// 3. Select what to replay.
let skippedInvalid = 0;
let skippedOld = 0;
const toSend = [];
for (const { partition, message } of collected) {
  if (Number(message.timestamp) < SINCE) {
    skippedOld += 1;
    continue;
  }
  if (!INCLUDE_INVALID && !looksLikeOrderPlaced(message.value)) {
    skippedInvalid += 1;
    continue;
  }
  if (toSend.length >= MAX) break;

  const headers = {};
  for (const [k, v] of Object.entries(message.headers || {})) {
    if (!k.startsWith("kafka_")) headers[k] = v;
  }
  headers["redriven-from"] = `${DLT_TOPIC}:${partition}:${message.offset}`;

  toSend.push({ key: message.key, value: message.value, headers });
}

console.log(
  `DLT records read=${collected.length} toReplay=${toSend.length} skippedInvalid=${skippedInvalid} skippedOld=${skippedOld}`
);

if (DRY_RUN || toSend.length === 0) {
  console.log(DRY_RUN ? "dry-run: nothing published." : "nothing to replay.");
  process.exit(0);
}

// 4. Publish back to the main topic.
const producer = kafka.producer({ allowAutoTopicCreation: false });
await producer.connect();
for (let i = 0; i < toSend.length; i += 200) {
  await producer.send({ topic: MAIN_TOPIC, acks: -1, messages: toSend.slice(i, i + 200) });
}
await producer.disconnect();
console.log(`redriven=${toSend.length} -> ${MAIN_TOPIC}`);