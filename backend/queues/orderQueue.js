import { Queue, Worker } from "bullmq";
import redis from "../config/redis.js";
import axios from "axios";

export const orderQueue = new Queue("order-events", { 
  connection: redis,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 2000,
    },
    removeOnComplete: true,
    removeOnFail: false,
  }
});

const orderWorker = new Worker("order-events", async (job) => {
  const { userId, items } = job.data;
  console.log(`BullMQ Order Worker processing [OrderPlaced] event for User: ${userId}`);
  
  try {
    const purchasedProductIds = items.map(item => item.product);
    
    // Clear the cache for this user natively over your secure Upstash channel
    await redis.del(`recs:${userId}`);
    
    console.log(`Sending purchase data vectors to Python ML backend for User: ${userId}`);
    
    const ML_SERVICE_URL = process.env.ML_SERVICE_URL;
    
    if (!ML_SERVICE_URL) {
      console.warn("ML_SERVICE_URL variable is missing on Railway dashboard! Skipping matrix stream.");
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
  connection: redis,
  concurrency: 5, // Limit concurrent jobs to prevent connection pool exhaustion
});

orderWorker.on("completed", (job) => 
  console.log(`✅ Order Event Job ${job.id} processed successfully.`)
);

orderWorker.on("failed", (job, err) => 
  console.error(`❌ Order Event Job ${job?.id} Failed: ${err.message}`)
);

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("Shutting down order worker gracefully...");
  await orderWorker.close();
  await orderQueue.close();
});
