/**
 * Needs a MongoDB REPLICA SET (transactions). Locally:
 *   docker compose -f docker-compose.infra.yml up -d mongo mongo-init
 *   pnpm test:outbox
 * Override with MONGO_TEST_URI (a throw-away database name is appended/used automatically).
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { randomUUID } from "node:crypto";
import mongoose from "mongoose";

import Outbox, { OUTBOX_STATUS } from "../models/outboxModel.js";
import Order from "../models/orderModel.js";
import {
  claimNextRow,
  markPublished,
  markFailedAttempt,
  releaseForConnectivity,
  requeueFailed,
  backoffMs,
} from "../outbox/outboxStore.js";
import { saveOrderWithOutbox } from "../outbox/orderOutbox.js";
import { buildOrderPlacedEvent, validateOrderPlaced, toMinorUnits, formatMinor } from "../events/orderPlaced.js";
import { classifyError, SendTimeoutError, PayloadSerializationError } from "../outbox/kafkaErrors.js";

const MONGO_URI = process.env.MONGO_TEST_URI || "mongodb://127.0.0.1:27017/?directConnection=true";
const DB_NAME = `outbox_test_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
const oid = () => new mongoose.Types.ObjectId();

async function seed(n, overrides = {}) {
  const docs = Array.from({ length: n }, (_, i) => ({
    eventId: randomUUID(),
    topic: "order.placed",
    partitionKey: String(oid()),
    payload: { i },
    status: OUTBOX_STATUS.PENDING,
    nextAttemptAt: new Date(Date.now() - 1_000),
    ...overrides,
  }));
  await Outbox.insertMany(docs);
  return docs;
}

function validOrderData(id = oid(), userId = oid()) {
  return {
    _id: id,
    orderItems: [
      { name: "A", image: "a.png", qty: 2, product: oid(), price: 49.99 },
      { name: "B", image: "b.png", qty: 1, product: oid(), price: 100 },
    ],
    user: userId,
    shippingAddress: { address: "1 Road", city: "Bhubaneswar", postalCode: "751001", country: "IN" },
    paymentMethod: "PayPal",
    itemsPrice: "199.98",
    taxPrice: "0.00",
    shippingPrice: "0.00",
    totalPrice: "199.98",
    isPaid: true,
    paidAt: new Date(),
  };
}

beforeAll(async () => {
  try {
    await mongoose.connect(MONGO_URI, { dbName: DB_NAME, serverSelectionTimeoutMS: 15_000 });
  } catch (err) {
    const safeUri = MONGO_URI.replace(/\/\/[^@]*@/, "//***@");
    throw new Error(
      `Cannot reach a writable MongoDB replica set at ${safeUri}. ` +
        `Run: docker compose -f docker-compose.infra.yml up -d mongo mongo-init (${err.message})`
    );
  }
  await Outbox.init();
  await Order.init();
});

afterAll(async () => {
  if (mongoose.connection.readyState === 1) {
    await mongoose.connection.dropDatabase();
  }
  await mongoose.disconnect();
});

beforeEach(async () => {
  await Outbox.deleteMany({});
  await Order.deleteMany({});
});

describe("claimNextRow", () => {
  it("concurrent claims never return the same row", async () => {
    await seed(60);
    const claimed = [];
    const worker = async () => {
      for (;;) {
        const row = await claimNextRow({ leaseMs: 30_000 });
        if (!row) return;
        claimed.push(row.eventId);
      }
    };
    await Promise.all(Array.from({ length: 10 }, worker));

    expect(claimed).toHaveLength(60);
    expect(new Set(claimed).size).toBe(60);
    expect(await Outbox.countDocuments({ status: OUTBOX_STATUS.PROCESSING })).toBe(60);
  });

  it("claims the oldest-due row first and ignores rows scheduled in the future", async () => {
    const [first, second] = await seed(2);
    await Outbox.updateOne({ eventId: first.eventId }, { nextAttemptAt: new Date(Date.now() - 5_000) });
    await seed(1, { nextAttemptAt: new Date(Date.now() + 60_000) });

    expect((await claimNextRow()).eventId).toBe(first.eventId);
    expect((await claimNextRow()).eventId).toBe(second.eventId);
    expect(await claimNextRow()).toBeNull();
  });

  it("rows created without an explicit nextAttemptAt are claimable (schema default)", async () => {
    await Outbox.create({
      eventId: randomUUID(),
      topic: "order.placed",
      partitionKey: "u1",
      payload: { ok: true },
    });
    const row = await claimNextRow();
    expect(row).not.toBeNull();
    expect(row.status).toBe(OUTBOX_STATUS.PROCESSING);
  });

  it("reclaims a PROCESSING row whose lease expired, but not a fresh one", async () => {
    const [stale] = await seed(1, {
      status: OUTBOX_STATUS.PROCESSING,
      lockedAt: new Date(Date.now() - 60_000),
      lockToken: "old-token",
    });
    await seed(1, {
      status: OUTBOX_STATUS.PROCESSING,
      lockedAt: new Date(Date.now() - 5_000),
      lockToken: "fresh-token",
    });

    const row = await claimNextRow({ leaseMs: 30_000 });
    expect(row.eventId).toBe(stale.eventId);
    expect(row.reclaimed).toBe(true);
    expect(row.lockToken).not.toBe("old-token");
    expect(await claimNextRow({ leaseMs: 30_000 })).toBeNull();
  });
});

describe("fencing", () => {
  it("a stale relay cannot overwrite a row that another relay re-claimed", async () => {
    await seed(1);
    const a = await claimNextRow({ leaseMs: 30_000 });
    // lease expires, relay B takes over
    const b = await claimNextRow({ now: new Date(Date.now() + 31_000), leaseMs: 30_000 });
    expect(b.eventId).toBe(a.eventId);
    expect(b.lockToken).not.toBe(a.lockToken);

    expect(await markPublished(a)).toBe(false); // A lost the lease
    expect((await Outbox.findOne({ eventId: a.eventId })).status).toBe(OUTBOX_STATUS.PROCESSING);
    expect(await markPublished(b)).toBe(true);
    expect((await Outbox.findOne({ eventId: a.eventId })).status).toBe(OUTBOX_STATUS.PUBLISHED);
  });

  it("a stale relay cannot push a published row back to PENDING or FAILED", async () => {
    await seed(1);
    const a = await claimNextRow({ leaseMs: 30_000 });
    const b = await claimNextRow({ now: new Date(Date.now() + 31_000), leaseMs: 30_000 });
    await markPublished(b);

    expect(await releaseForConnectivity(a, { error: new Error("x") })).toBe(false);
    expect((await markFailedAttempt(a, { error: new Error("x"), permanent: true })).updated).toBe(false);
    expect((await Outbox.findOne({ eventId: a.eventId })).status).toBe(OUTBOX_STATUS.PUBLISHED);
  });
});

describe("failure handling", () => {
  it("connectivity failures never consume attempts and never reach FAILED", async () => {
    await seed(1);
    for (let i = 0; i < 20; i++) {
      const row = await claimNextRow({ now: new Date(Date.now() + i * 10_000) });
      expect(row).not.toBeNull();
      expect(await releaseForConnectivity(row, { error: new Error("ECONNREFUSED"), retryInMs: 0 })).toBe(true);
    }
    const doc = await Outbox.findOne({});
    expect(doc.status).toBe(OUTBOX_STATUS.PENDING);
    expect(doc.attempts).toBe(0);
  });

  it("unknown errors back off and become FAILED after maxAttempts", async () => {
    await seed(1);
    for (let i = 1; i <= 3; i++) {
      const row = await claimNextRow({ now: new Date(Date.now() + i * 3_600_000) });
      const res = await markFailedAttempt(row, { error: new Error("weird"), maxAttempts: 3 });
      expect(res.updated).toBe(true);
      expect(res.status).toBe(i < 3 ? OUTBOX_STATUS.PENDING : OUTBOX_STATUS.FAILED);
    }
    const doc = await Outbox.findOne({});
    expect(doc.status).toBe(OUTBOX_STATUS.FAILED);
    expect(doc.attempts).toBe(3);
    expect(doc.failedAt).toBeInstanceOf(Date);
  });

  it("permanent errors go straight to FAILED and can be requeued", async () => {
    const [d] = await seed(1);
    const row = await claimNextRow();
    await markFailedAttempt(row, { error: new Error("too large"), permanent: true });
    expect((await Outbox.findOne({ eventId: d.eventId })).status).toBe(OUTBOX_STATUS.FAILED);

    expect(await requeueFailed()).toBe(1);
    const again = await claimNextRow();
    expect(again.eventId).toBe(d.eventId);
    expect(again.attempts).toBe(0);
  });

  it("backoff grows exponentially and is capped", () => {
    const opts = { baseMs: 1000, maxMs: 60_000, jitter: 0.2 };
    expect(backoffMs(1, opts)).toBeGreaterThanOrEqual(800);
    expect(backoffMs(1, opts)).toBeLessThanOrEqual(1200);
    expect(backoffMs(3, opts)).toBeGreaterThanOrEqual(3200);
    expect(backoffMs(30, opts)).toBeLessThanOrEqual(72_000);
  });
});

describe("kafka error classification", () => {
  it("unwraps KafkaJSNumberOfRetriesExceeded", () => {
    const err = { name: "KafkaJSNumberOfRetriesExceeded", originalError: { name: "KafkaJSConnectionError" } };
    expect(classifyError(err)).toBe("connectivity");
  });
  it("oversized record is permanent even when wrapped", () => {
    expect(classifyError({ name: "KafkaJSNumberOfRetriesExceeded", originalError: { type: "MESSAGE_TOO_LARGE" } })).toBe(
      "permanent"
    );
  });
  it("missing topic / broker down / timeouts are connectivity", () => {
    expect(classifyError({ type: "UNKNOWN_TOPIC_OR_PARTITION" })).toBe("connectivity");
    expect(classifyError(new SendTimeoutError(100))).toBe("connectivity");
    expect(classifyError(new Error("connect ECONNREFUSED 127.0.0.1:9092"))).toBe("connectivity");
  });
  it("unserialisable payload is permanent; random errors are unknown", () => {
    expect(classifyError(new PayloadSerializationError(new Error("circular")))).toBe("permanent");
    expect(classifyError(new Error("boom"))).toBe("unknown");
  });
});

describe("order + outbox are one atomic unit", () => {
  it("commits both", async () => {
    const session = await mongoose.startSession();
    const eventId = randomUUID();
    const data = validOrderData();
    try {
      await session.withTransaction(async () => {
        await saveOrderWithOutbox({ session, orderData: data, eventId, traceCarrier: {} });
      });
    } finally {
      await session.endSession();
    }
    expect(await Order.countDocuments({ _id: data._id })).toBe(1);
    const row = await Outbox.findOne({ eventId }).lean();
    expect(row.status).toBe(OUTBOX_STATUS.PENDING);
    expect(row.partitionKey).toBe(String(data.user));
    expect(row.payload.items[0]).toMatchObject({ quantity: 2, unitPrice: "49.99" });
    expect(row.payload.totalPrice).toBe("199.98");
    expect(() => validateOrderPlaced(row.payload)).not.toThrow();
  });

  it("rolls BOTH back when the outbox insert fails (duplicate eventId)", async () => {
    const eventId = randomUUID();
    await Outbox.create({ eventId, topic: "order.placed", partitionKey: "x", payload: {} });
    const data = validOrderData();

    const session = await mongoose.startSession();
    await expect(
      session.withTransaction(async () => {
        await saveOrderWithOutbox({ session, orderData: data, eventId, traceCarrier: {} });
      })
    ).rejects.toThrow();
    await session.endSession();

    expect(await Order.countDocuments({ _id: data._id })).toBe(0);
    expect(await Outbox.countDocuments({ eventId })).toBe(1); // only the pre-existing row
  });

  it("rolls BOTH back when something later in the callback throws", async () => {
    const eventId = randomUUID();
    const data = validOrderData();
    const session = await mongoose.startSession();
    await expect(
      session.withTransaction(async () => {
        await saveOrderWithOutbox({ session, orderData: data, eventId, traceCarrier: {} });
        throw new Error("boom after both writes");
      })
    ).rejects.toThrow("boom");
    await session.endSession();

    expect(await Order.countDocuments({ _id: data._id })).toBe(0);
    expect(await Outbox.countDocuments({ eventId })).toBe(0);
  });

  it("aborts the order when the event cannot be built faithfully", async () => {
    const eventId = randomUUID();
    const data = validOrderData();
    data.orderItems[0].price = 49.999; // 3 decimals -> would silently change the ledger
    data.totalPrice = "199.98";

    const session = await mongoose.startSession();
    await expect(
      session.withTransaction(async () => {
        await saveOrderWithOutbox({ session, orderData: data, eventId, traceCarrier: {} });
      })
    ).rejects.toThrow(/OutboxPayloadInvalid/);
    await session.endSession();
    expect(await Order.countDocuments({ _id: data._id })).toBe(0);
  });
});

describe("order.placed v1 contract", () => {
  const base = () => ({
    eventId: randomUUID(),
    order: {
      _id: oid(),
      user: oid(),
      createdAt: new Date("2026-11-01T08:00:00.000Z"),
      totalPrice: 0.3,
      orderItems: [{ product: oid(), qty: 3, price: 0.1 }],
    },
  });

  it("is exact for float-hostile prices (0.1 * 3)", () => {
    const evt = buildOrderPlacedEvent(base());
    expect(evt.totalPrice).toBe("0.30");
    expect(evt.items[0].unitPrice).toBe("0.10");
    expect(evt.createdAt).toBe("2026-11-01T08:00:00.000Z");
  });

  it("rejects a total that disagrees with the items", () => {
    const b = base();
    b.order.totalPrice = 0.31;
    expect(() => buildOrderPlacedEvent(b)).toThrow(/OutboxPayloadInvalid/);
  });

  it("rejects a poison payload", () => {
    expect(() => validateOrderPlaced({ corrupt: true })).toThrow(/OutboxPayloadInvalid/);
  });

  it("money helpers", () => {
    expect(toMinorUnits(16999, "x")).toBe(1699900);
    expect(formatMinor(5)).toBe("0.05");
    expect(formatMinor(1699900)).toBe("16999.00");
    expect(() => toMinorUnits(-1, "x")).toThrow();
  });
});