import { stockDecrementQueue } from "../queues/stockDecrementQueue.js";
import { mlRecommendationQueue } from "../queues/mlRecommendationQueue.js";
import { recommendationQueue } from "../queues/recommendationQueue.js";
import { getOutboxStats, requeueFailed } from "../outbox/outboxStore.js"; // NEW

const replayableQueues = [
  { name: "stock-decrement", queue: stockDecrementQueue },
  { name: "ml-recommendation", queue: mlRecommendationQueue },
  { name: "recommendation-updates", queue: recommendationQueue },
];

export const retryFailedJobs = async (req, res) => {
  try {
    const breakdown = {};
    let foundCount = 0;
    let requeuedCount = 0;

    for (const { name, queue } of replayableQueues) {
      const failedJobs = await queue.getFailed();
      const results = await Promise.allSettled(failedJobs.map((job) => job.retry()));
      const requeued = results.filter((r) => r.status === "fulfilled").length;

      breakdown[name] = {
        found: failedJobs.length,
        requeued,
        errors: failedJobs.length - requeued,
      };

      foundCount += failedJobs.length;
      requeuedCount += requeued;
    }

    if (foundCount === 0) {
      return res.status(200).json({
        message: "No failed jobs found in the DLQ.",
        requeuedCount: 0,
        breakdown,
      });
    }

    res.status(200).json({
      message: `Requeued ${requeuedCount}of ${foundCount}failed jobs.`,
      requeuedCount,
      breakdown,
    });
  } catch (error) {
    console.error(`[Queue Admin Error]Failed to replay DLQ: ${error.message}`);
    res.status(500).json({ error: "Server error while attempting to retry failed jobs." });
  }
};

// NEW: GET /api/queues/outbox/stats
export const outboxStats = async (req, res) => {
  try {
    const stats = await getOutboxStats();
    if (stats.failed > 0) {
      console.error(`[ALERT] ${stats.failed} outbox row(s) are FAILED (last: ${stats.lastFailed?.eventId})`);
    }
    res.status(200).json(stats);
  } catch (error) {
    console.error(`[Queue Admin Error]Failed to read outbox stats: ${error.message}`);
    res.status(500).json({ error: "Server error while reading outbox stats." });
  }
};

// NEW: POST /api/queues/outbox/requeue-failed   body (optional): { "eventIds": ["..."] }
export const requeueFailedOutbox = async (req, res) => {
  try {
    const eventIds = Array.isArray(req.body?.eventIds)
      ? req.body.eventIds.filter((id) => typeof id === "string")
      : undefined;
    const requeued = await requeueFailed({ eventIds });
    res.status(200).json({ message: `Requeued ${requeued} FAILED outbox row(s).`, requeued });
  } catch (error) {
    console.error(`[Queue Admin Error]Failed to requeue outbox rows: ${error.message}`);
    res.status(500).json({ error: "Server error while requeueing outbox rows." });
  }
};