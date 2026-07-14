import { Queue, Worker } from "bullmq";
import redis, { createRedisConnection } from "../config/redis.js";
import axios from "axios";
import Product from "../models/productModel.js"; 
export const orderQueue = new Queue("order-events", {
  connection: createRedisConnection(),
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: true,
    removeOnFail: false,
  }
});

const orderWorker = new Worker("order-events", async (job) => {
  // Extract orderId alongside userId and items for better tracing
  const { userId, items, orderId } = job.data; 
  console.log(`BullMQ Order Worker processing [OrderPlaced] event for User: ${userId}`);

  try {
    //(Eventual Consistency)
    console.log(`Synchronizing MongoDB inventory for Order: ${orderId || 'unknown'}`);
    const bulkOperations = items.map((item) => ({
      updateOne: {
        filter: { _id: item.product },
        update: { $inc: { countInStock: -item.qty } } 
      }
    }));
    
    if (bulkOperations.length > 0) {
      await Product.bulkWrite(bulkOperations);
    }
    
    const purchasedProductIds = items.map(item => item.product);
    await redis.del(`recs:${userId}`);
    
    console.log(`Sending purchase data vectors to Python ML backend for User: ${userId}`);
    const ML_SERVICE_URL = process.env.ML_SERVICE_URL;
    
    if (!ML_SERVICE_URL) {
      console.warn("ML_SERVICE_URL variable is missing! Skipping matrix stream.");
      return; 
    }
    
    await axios.post(`${ML_SERVICE_URL}/update-matrix`, {
      userId,
      productIds: purchasedProductIds,
      eventType: "PURCHASE"
    }, { timeout: 5000 });
    
    console.log(`Asynchronous post-purchase operations finalized for User: ${userId}`);

  } catch (error) {
    console.error(`Order Worker Error: ${error.message}`);
    throw error; 
  }
}, {
  connection: createRedisConnection(),
  concurrency: 5,
});

orderWorker.on("completed", (job) => console.log(`Order Event Job ${job.id} processed successfully.`));
orderWorker.on("failed", (job, err) => {
  const orderId = job?.data?.orderId || 'UNKNOWN_ORDER';
  const userId = job?.data?.userId || 'UNKNOWN_USER';

  console.error(`\n [CRITICAL DLQ ALERT] Background Worker Failure`);
  console.error(`=> Job ID: ${job?.id}`);
  console.error(`=> Order ID: ${orderId}`);
  console.error(`=> User ID: ${userId}`);
  console.error(`=> Reason: ${err.message}\n`);
});

process.on("SIGTERM", async () => {
  await orderWorker.close();
  await orderQueue.close();
});