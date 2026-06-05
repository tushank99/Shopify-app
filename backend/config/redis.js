import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

// Connect to the Docker Redis container running on localhost:6379
const redis = new Redis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: process.env.REDIS_PORT || 6379,
  maxRetriesPerRequest: null, // BullMQ explicitly requires this setting to manage job failures safely
});

redis.on("connect", () => {
  console.log("Redis Cache & Queue Cluster Connected Successfully!");
});

redis.on("error", (err) => {
  console.error("Redis Connection Error:", err.message);
});

export default redis;