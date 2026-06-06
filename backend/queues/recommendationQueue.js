import { Queue, Worker } from "bullmq";
import axios from "axios";
import Product from "../models/productModel.js";
import redis, { redisConfig } from "../config/redis.js"; // 🟢 Import config object

export const recommendationQueue = new Queue("recommendation-updates", {
  connection: redisConfig,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: true,
    removeOnFail: false,
  }
});

const worker = new Worker("recommendation-updates", async (job) => {
  const { userId } = job.data;
  console.log(`BullMQ Worker processing background update task for User: ${userId}`);
  try {
    const ML_SERVICE_URL = process.env.ML_SERVICE_URL || "http://127.0.0.1:8000";
    const response = await axios.get(`${ML_SERVICE_URL}/recommend/${userId}`, { timeout: 3000 });
    const recommendedIds = response.data.recommendations;
    
    if (recommendedIds && recommendedIds.length > 0) {
      const products = await Product.find({ _id: { $in: recommendedIds } }).populate("category");
      const orderedProducts = recommendedIds.map((id) => products.find((prod) => prod._id.toString() === id)).filter(Boolean);
      
      // Using main client for data write execution is perfect
      await redis.set(`recs:${userId}`, JSON.stringify(orderedProducts), "EX", 86400);
      console.log(`BullMQ Worker successfully hot-swapped Redis cache vectors for User: ${userId}`);
    }
  } catch (error) {
    console.error(`BullMQ Worker failed to update user profile cache: ${error.message}`);
    throw error;
  }
}, {
  connection: redisConfig, 
  concurrency: 5,
});

worker.on("completed", (job) => console.log(`Background Job ${job.id} finalized cleanly.`));
worker.on("failed", (job, err) => console.error(`Background Job ${job.id} dropped: ${err.message}`));

process.on("SIGTERM", async () => {
  await worker.close();
  await recommendationQueue.close();
});