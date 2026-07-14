import redisClient from '../config/redis.js';
export const rateLimiter = (maxRequests, windowSeconds) => {
  return async (req, res, next) => {
    
    let identifier = '';
    let namespace = '';

    if (req.user && req.user._id) {
      identifier = req.user._id.toString();
      namespace = 'user';
    } else {
      identifier = req.ip || 'anonymous';
      namespace = 'ip';
    }

    const normalizedRoute = req.baseUrl + req.path;
    const redisKey = `ratelimit:${namespace}:${identifier}:${normalizedRoute}`;

    const now = Date.now();
    const clearBefore = now - (windowSeconds * 1000);

    //  Fail-Open Architecture Strategy
    // If Redis encounters a connectivity blip or timeout, we catch the exception and fall open
    try {
      //  Race Condition Mitigation: Execute commands as an Atomic Unit via Multi/Exec pipeline
      const pipeline = redisClient.multi();
      
      // Remove elements older than the current sliding window threshold
      pipeline.zremrangebyscore(redisKey, 0, clearBefore);
      // Add the current request timestamp as both value and score
      pipeline.zadd(redisKey, now, now);
      // Retrieve total remaining elements in the sliding window log
      pipeline.zcard(redisKey);
      // Keep the Redis memory footprint clean by resetting key TTL on every request
      pipeline.expire(redisKey, windowSeconds);

      const results = await pipeline.exec();
      
      const totalRequestsInWindow = results[2][1];
      const remainingRequests = Math.max(0, maxRequests - totalRequestsInWindow);

      res.setHeader('X-RateLimit-Limit', maxRequests);
      res.setHeader('X-RateLimit-Remaining', remainingRequests);

      if (totalRequestsInWindow > maxRequests) {
        res.setHeader('Retry-After', windowSeconds);
        return res.status(429).json({
          status: 'fail',
          message: 'Too many requests. High concurrency threshold exceeded. Please retry later.'
        });
      }

      next();
    } catch (error) {
      // Fallback Strategy: Log infrastructure error to stdout, shield the user layout, and bypass gate
      console.error(`[RateLimiter Infrastructure Alert] Fail-Open Triggered due to Redis outage: ${error.message}`);
      next(); 
    }
  };
};