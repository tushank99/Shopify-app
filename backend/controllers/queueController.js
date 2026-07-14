import { orderQueue } from '../queues/orderQueue.js';

export const retryFailedJobs = async (req, res) => {
  try {
    //  Query BullMQ for jobs trapped in the DLQ state
    const failedJobs = await orderQueue.getFailed();

    if (failedJobs.length === 0) {
      return res.status(200).json({ message: "No failed jobs found in the DLQ." });
    }

    //  Execute retry() on all dead jobs in parallel
    const retryPromises = failedJobs.map((job) => job.retry());
    await Promise.all(retryPromises);

    res.status(200).json({
      message: `Successfully resuscitated and requeued ${failedJobs.length} failed jobs.`,
      requeuedCount: failedJobs.length
    });
  } catch (error) {
    console.error(`[Queue Admin Error] Failed to replay DLQ: ${error.message}`);
    res.status(500).json({ error: "Server error while attempting to retry failed jobs." });
  }
};