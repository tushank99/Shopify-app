import { Queue, Worker } from "bullmq";
import Product from "../models/productModel.js";
import redis from "../config/redis.js";
import axios from "axios";

const redisConnection = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: process.env.REDIS_PORT || 6379,
};

// 1. Initialize the Order Event Queue Broker
export const orderQueue = new Queue("order-events", { connection: redisConnection });

// 2. Setup the Worker to process "OrderPlaced" events asynchronously
const orderWorker = new Worker(
  "order-events",
  async (job) => {
    const { userId, items } = job.data;
    console.log(`BullMQ Order Worker processing [OrderPlaced] event for User: ${userId}`);

    try {
      // Extract product IDs to understand what the user just purchased
      const purchasedProductIds = items.map(item => item.product);
      
      // Instantly wipe old cache—their tastes have significantly shifted due to a purchase
      await redis.del(`recs:${userId}`);

      // Ping the Python ML sidecar to adjust user-item collaborative feature weights
      console.log(`Sending purchase data vectors to Python ML backend for User: ${userId}`);
      const ML_SERVICE_URL = process.env.ML_SERVICE_URL || "http://127.0.0.1:8000";
      await axios.post(`${ML_SERVICE_URL}/update-matrix`, {
        userId,
        productIds: purchasedProductIds,
        eventType: "PURCHASE"
      }, { timeout: 3000 }).catch(e => console.log(" Python Sidecar matrix receiver endpoint not active yet. Skipping matrix update."));

      console.log(`Asynchronous post-purchase operations finalized for User: ${userId}`);
    } catch (error) {
      console.error(`Order Worker Error: ${error.message}`);
      throw error;
    }
  },
  { connection: redisConnection }
);

orderWorker.on("completed", (job) => console.log(`Order Event Job ${job.id} processed successfully.`));