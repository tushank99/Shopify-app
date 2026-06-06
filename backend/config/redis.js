import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

let redis;

if (process.env.REDIS_URL) {
  
  const isSecure = process.env.REDIS_URL.startsWith("rediss://");

  redis = new Redis(process.env.REDIS_URL, {
    maxRetriesPerRequest: null, // Critical flag for BullMQ compatibility
    tls: isSecure ? { rejectUnauthorized: false } : undefined, // Handles Upstash TLS handshake
  });
} else {
  redis = new Redis({
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: process.env.REDIS_PORT || 6379,
    maxRetriesPerRequest: null,
  });
}

redis.on("connect", () => {
  console.log("Redis Cache & Queue Cluster Connected Successfully! ");
});

redis.on("error", (err) => {
  console.error("Redis Connection Error:", err.message);
});

export default redis;