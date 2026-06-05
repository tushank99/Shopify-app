import { Queue, Worker } from "bullmq";
import axios from "axios";
import Product from "../models/productModel.js";
import redis from "../config/redis.js"; 


const redisConnection = process.env.REDIS_URL 
  ? process.env.REDIS_URL // Uses Upstash/Cloud connection string in production
  : {
      host: process.env.REDIS_HOST || "127.0.0.1",
      port: process.env.REDIS_PORT || 6379,
    };

// 1. Initialize the Producer Queue
export const recommendationQueue = new Queue("recommendation-updates", {
  connection: redisConnection,
});

// 2. Setup the Worker to handle asynchronous tasks background execution
const worker = new Worker(
  "recommendation-updates",
  async (job) => {
    const { userId } = job.data;
    console.log(`BullMQ Worker processing background update task for User: ${userId}`);

    try {
      // Direct call to Python ML sidecar to force-calculate updated array IDs
      const response = await axios.get(`http://127.0.0.1:8000/recommend/${userId}`, { timeout: 3000 });
      const recommendedIds = response.data.recommendations;

      if (recommendedIds && recommendedIds.length > 0) {
        // Fetch products maintaining sequence order
        const products = await Product.find({ _id: { $in: recommendedIds } }).populate("category");
        const orderedProducts = recommendedIds
          .map(id => products.find(prod => prod._id.toString() === id))
          .filter(Boolean);

        // Update the cache directly
        await redis.set(`recs:${userId}`, JSON.stringify(orderedProducts), "EX", 86400);
        console.log(`✅ BullMQ Worker successfully hot-swapped Redis cache vectors for User: ${userId}`);
      }
    } catch (error) {
      console.error(`BullMQ Worker failed to update user profile cache background execution: ${error.message}`);
      throw error; // Let BullMQ track the job failure state automatically
    }
  },
  { connection: redisConnection }
);

// Worker Event Listener Hooks
worker.on("completed", (job) => console.log(`Background Job ${job.id} finalized cleanly.`));
worker.on("failed", (job, err) => console.error(`Background Job ${job.id} dropped: ${err.message}`));