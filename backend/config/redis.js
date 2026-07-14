import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

const url = process.env.REDIS_URL || "";

const needsTls = url.startsWith("rediss://") || url.includes("upstash.io");

// Shared connection options for ALL ioredis clients (standalone + BullMQ)
export const redisConfig = {
  maxRetriesPerRequest: null, // REQUIRED by BullMQ
  enableReadyCheck: false,
  tls: needsTls ? { rejectUnauthorized: false } : undefined,
  connectTimeout: 10000,
  // Back off on reconnect instead of hammering the server in a tight loop.
  retryStrategy: (times) => Math.min(times * 200, 5000),
};


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

const LUA_RESERVE_STOCK = `
  local stock = redis.call('GET', KEYS[1])
  if not stock then return -2 end
  stock = tonumber(stock)
  local requested = tonumber(ARGV[1])
  if stock < requested then return -1 end
  redis.call('DECRBY', KEYS[1], requested)
  return stock - requested
`;

export let reserveStockSha = null;

redis.on("connect", async () => {
  console.log("Main Redis Cache Client Connected!");
  try {
    // Load script into Redis memory on boot and save the SHA
    reserveStockSha = await redis.script("LOAD", LUA_RESERVE_STOCK);
    console.log("Lua Reservation Script cached successfully.");
  } catch (error) {
    console.error("Failed to load Lua script:", error);
  }
});

redis.on("error", (err) => console.error("Main Redis Client Error:", err.message));

export const redisClient = redis;
export default redis;
