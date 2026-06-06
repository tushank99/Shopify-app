import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

// Shared connection options for ALL ioredis clients (standalone + BullMQ)
export const redisConfig = {
  maxRetriesPerRequest: null, // REQUIRED by BullMQ
  enableReadyCheck: false,
  tls:
    process.env.REDIS_URL && process.env.REDIS_URL.startsWith("rediss://")
      ? { rejectUnauthorized: false }
      : undefined,
  connectTimeout: 10000,
};

// Factory: builds a FULLY-configured connection (with the real target URL/host).
// Each Queue/Worker must call this so it gets its OWN dedicated connection.
// This fixes two problems:
//   1. Queues/workers were getting a config object with NO host/url, so ioredis
//      silently fell back to 127.0.0.1:6379 -> ECONNREFUSED loop on Railway.
//   2. Sharing one socket across Queue + blocking Worker + cache caused the
//      ECONNRESET cross-talk / reconnect loop on Upstash.
export const createRedisConnection = () =>
  process.env.REDIS_URL
    ? new Redis(process.env.REDIS_URL, redisConfig)
    : new Redis({
        host: process.env.REDIS_HOST || "127.0.0.1",
        port: parseInt(process.env.REDIS_PORT) || 6379,
        ...redisConfig,
      });

// Main standalone client for regular API cache get/set requests
const redis = createRedisConnection();

redis.on("connect", () => console.log("Main Redis Cache Client Connected!"));
redis.on("error", (err) => console.error(" Main Redis Client Error:", err.message));

export default redis;
