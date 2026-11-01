import { Queue, Worker, UnrecoverableError } from "bullmq";
import mongoose from "mongoose";
import { createRedisConnection } from "../config/redis.js";
import Product from "../models/productModel.js";
import ProcessedStockOrder from "../models/processedStockOrderModel.js";

const OBJECT_ID_REGEX = /^[a-f\d]{24}$/i;

// Ensure unique index is built once at startup, not re-triggered per job
const indexReadyPromise = ProcessedStockOrder.init();

export const stockDecrementQueue = new Queue("stock-decrement", {
  connection: createRedisConnection(),
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 2000 },
    removeOnComplete: true,
    removeOnFail: false,
  },
});

const stockWorker = new Worker(
  "stock-decrement",
  async (job) => {
    const { orderId, items } = job.data || {};

    if (
      typeof orderId !== "string" ||
      !OBJECT_ID_REGEX.test(orderId) ||
      !Array.isArray(items) ||
      items.length === 0
    ) {
      throw new UnrecoverableError(`InvalidJobPayload:${job.id}`);
    }

    const qtyByProduct = new Map();
    for (const item of items) {
      const productId = item?.productId || item?.product;
      const qty = Number(item?.qty);
      if (
        typeof productId !== "string" ||
        !OBJECT_ID_REGEX.test(productId) ||
        !Number.isInteger(qty) ||
        qty < 1
      ) {
        throw new UnrecoverableError(`InvalidJobItem:${job.id}`);
      }
      qtyByProduct.set(productId, (qtyByProduct.get(productId) || 0) + qty);
    }

    // Sort deterministically by productId to prevent multi-document transaction deadlocks
    const orderedItems = [...qtyByProduct.entries()].sort(([a], [b]) =>
      a < b ? -1 : a > b ? 1 : 0
    );

    await indexReadyPromise;

    const session = await mongoose.startSession();
    try {
      await session.withTransaction(
        async () => {
          await ProcessedStockOrder.create(
            [{ orderId, processedAt: new Date() }],
            { session }
          );

          for (const [productId, qty] of orderedItems) {
            const result = await Product.updateOne(
              { _id: productId, countInStock: { $gte: qty } },
              { $inc: { countInStock: -qty } },
              { session }
            );

            if (result.modifiedCount !== 1) {
              throw new UnrecoverableError(`InsufficientStock:${productId}`);
            }
          }
        },
        { writeConcern: { w: "majority" } }
      );
    } catch (error) {
      if (error?.code === 11000) {
        console.log(`Stock already processed for order ${orderId}; skipping.`);
        return;
      }
      throw error;
    } finally {
      await session.endSession();
    }
  },
  {
    connection: createRedisConnection(),
    concurrency: 5,
  }
);

stockWorker.on("completed", (job) => {
  console.log(`Stock decrement job ${job.id} completed successfully.`);
});

stockWorker.on("failed", (job, error) => {
  console.error(
    `[STOCK DLQ ALERT] job=${job?.id} order=${job?.data?.orderId} ` +
      `attempt=${job?.attemptsMade}/${job?.opts?.attempts} reason=${error.message}`
  );
});

stockWorker.on("error", (error) => {
  console.error(`Stock worker error: ${error.message}`);
});

process.on("SIGTERM", async () => {
  await stockWorker.close();
  await stockDecrementQueue.close();
});