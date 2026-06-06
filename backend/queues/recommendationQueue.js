import { Queue, Worker } from "bullmq";
import axios from "axios";
import Product from "../models/productModel.js";
import redis from "../config/redis.js";

// 1. Initialize the Queue using the shared connection client
export const recommendationQueue = new Queue("recommendation-updates", {
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

// 2. Setup the Worker to process background calculations smoothly
const worker = new Worker(
  "recommendation-updates",
  async (job) => {
    const { userId } = job.data;
    console.log(`BullMQ Worker processing background update task for User: ${userId}`);
    
    try {
      
      const ML_SERVICE_URL = process.env.ML_SERVICE_URL || "http://127.0.0.1:8000";
      
      const response = await axios.get(`${ML_SERVICE_URL}/recommend/${userId}`, { timeout: 3000 });
      const recommendedIds = response.data.recommendations;
      
      if (recommendedIds && recommendedIds.length > 0) {
        const products = await Product.find({ _id: { $in: recommendedIds } }).populate("category");
        const orderedProducts = recommendedIds
          .map((id) => products.find((prod) => prod._id.toString() === id))
          .filter(Boolean);
          
        await redis.set(`recs:${userId}`, JSON.stringify(orderedProducts), "EX", 86400);
        console.log(`✅ BullMQ Worker successfully hot-swapped Redis cache vectors for User: ${userId}`);
      }
    } catch (error) {
      console.error(`❌ BullMQ Worker failed to update user profile cache background execution: ${error.message}`);
      throw error;
    }
  },
  { 
    connection: redis,
    concurrency: 5, // Limit concurrent jobs to prevent connection pool exhaustion
  }
);

worker.on("completed", (job) => console.log(`✅ Background Job ${job.id} finalized cleanly.`));
worker.on("failed", (job, err) => console.error(`❌ Background Job ${job.id} dropped: ${err.message}`));

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("Shutting down recommendation worker gracefully...");
  await worker.close();
  await recommendationQueue.close();
});
