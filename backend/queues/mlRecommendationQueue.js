import { Queue, Worker } from "bullmq";
import axios from "axios";
import { createRedisConnection, redisClient } from "../config/redis.js";

export const mlRecommendationQueue = new Queue("ml-recommendation", {
  connection: createRedisConnection(),
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 2000 },
    removeOnComplete: true,
    removeOnFail: false,
  },
});

const mlWorker = new Worker(
  "ml-recommendation",
  async (job) => {
    const { userId, orderId, productIds } = job.data;

    const ML_SERVICE_URL = process.env.ML_SERVICE_URL;

    if (ML_SERVICE_URL) {
      await axios.post(
        `${ML_SERVICE_URL}/update-matrix`,
        { userId, orderId, productIds, eventType: "PURCHASE" },
        { timeout: 5000 }
      );
    } else {
      console.warn("ML_SERVICE_URL variable is missing! Skipping matrix update.");
    }

    // Invalidate AFTER the matrix update so a concurrent read cannot re-cache stale data.
    await redisClient.del(`recs:${userId}`);
  },
  {
    connection: createRedisConnection(),
    concurrency: 5,
  }
);

mlWorker.on("completed", (job) => {
  console.log(`ML recommendation job ${job.id} completed successfully.`);
});

mlWorker.on("failed", (job, error) => {
  console.error(
    `ML recommendation job ${job?.id} order=${job?.data?.orderId} failed: ${error.message}`
  );
});

mlWorker.on("error", (error) => {
  console.error(`ML worker error: ${error.message}`);
});

process.on("SIGTERM", async () => {
  await mlWorker.close();
  await mlRecommendationQueue.close();
});