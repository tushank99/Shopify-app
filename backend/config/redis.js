import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

let redis;

if (process.env.REDIS_URL) {
  
  const isSecure = process.env.REDIS_URL.startsWith("rediss://");

  redis = new Redis(process.env.REDIS_URL, {
    // BullMQ compatibility
    maxRetriesPerRequest: null,
    
    // Upstash connection stability
    tls: isSecure ? { rejectUnauthorized: false } : undefined,
    
    // Socket configuration - prevent idle timeouts
    socket: {
      keepAlive: 30 * 1000, // Send keep-alive every 30s
      noDelay: true,
      reconnectStrategy: (times) => {
        const delay = Math.min(times * 50, 2000); // Backoff: 50ms, 100ms, ..., max 2s
        return delay;
      },
    },
    
    // Connection pooling
    lazyConnect: false,
    enableReadyCheck: true,
    enableOfflineQueue: true,
    
    // Operation timeouts
    commandTimeout: 5000, // 5s timeout for commands
    connectTimeout: 10000, // 10s timeout for connection
    maxRedirections: 16,
    retryStrategy: (times) => {
      const delay = Math.min(times * 50, 2000);
      return delay;
    },
  });
} else {
  redis = new Redis({
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: process.env.REDIS_PORT || 6379,
    maxRetriesPerRequest: null,
    socket: {
      keepAlive: 30 * 1000,
      noDelay: true,
    },
  });
}

redis.on("connect", () => {
  console.log("✅ Redis Cache & Queue Cluster Connected Successfully! ");
});

redis.on("error", (err) => {
  console.error("❌ Redis Connection Error:", err.message);
});

redis.on("close", () => {
  console.log("⚠️ Redis connection closed");
});

redis.on("reconnecting", () => {
  console.log("🔄 Redis reconnecting...");
});

export default redis;