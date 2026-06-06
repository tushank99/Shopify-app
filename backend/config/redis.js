import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

export const redisConfig = process.env.REDIS_URL
  ? {
      maxRetriesPerRequest: null,
      tls: process.env.REDIS_URL.startsWith("rediss://") ? { rejectUnauthorized: false } : undefined,
      connectTimeout: 10000,
    }
  : {
      host: process.env.REDIS_HOST || "127.0.0.1",
      port: parseInt(process.env.REDIS_PORT) || 6379,
      maxRetriesPerRequest: null,
    };

// This remains your main standalone client for regular API cache get/set requests
const redis = process.env.REDIS_URL 
  ? new Redis(process.env.REDIS_URL, redisConfig)
  : new Redis(redisConfig);

redis.on("connect", () => console.log("Main Redis Cache Client Connected!"));
redis.on("error", (err) => console.error(" Main Redis Client Error:", err.message));

export default redis;