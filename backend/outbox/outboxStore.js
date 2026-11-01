import { randomUUID } from "node:crypto";
import Outbox, { OUTBOX_STATUS } from "../models/outboxModel.js";

/**
 * All state transitions of the outbox. Every transition that happens AFTER a claim is
 * fenced on (status=PROCESSING, lockToken): if the lease expired and another relay re-claimed
 * the row, the stale relay's update matches nothing (returns false) and cannot overwrite it.
 */

const fence = (row) => ({
  _id: row._id,
  status: OUTBOX_STATUS.PROCESSING,
  lockToken: row.lockToken,
});

/**
 * Atomically claim ONE row. Two index-friendly paths:
 *   1. reclaim a PROCESSING row whose lease expired (relay crashed/stalled),
 *   2. claim the oldest-due PENDING row.
 * findOneAndUpdate is atomic per document, so concurrent callers can never get the same row
 * unless its lease genuinely expired in between (fencing then protects the final write).
 */
export async function claimNextRow({ now = new Date(), leaseMs = 30_000 } = {}) {
  const leaseThreshold = new Date(now.getTime() - leaseMs);
  const lockToken = randomUUID();
  // Fresh object per query: Mongoose may cast/mutate update documents in place.
  const claim = () => ({ $set: { status: OUTBOX_STATUS.PROCESSING, lockedAt: now, lockToken } });

  const reclaimed = await Outbox.findOneAndUpdate(
    { status: OUTBOX_STATUS.PROCESSING, lockedAt: { $lte: leaseThreshold } },
    claim(),
    { sort: { lockedAt: 1 }, new: true }
  ).lean();
  if (reclaimed) return { ...reclaimed, reclaimed: true };

  return Outbox.findOneAndUpdate(
    { status: OUTBOX_STATUS.PENDING, nextAttemptAt: { $lte: now } },
    claim(),
    { sort: { nextAttemptAt: 1, createdAt: 1 }, new: true }
  ).lean();
}

export async function markPublished(row, now = new Date()) {
  const res = await Outbox.updateOne(fence(row), {
    $set: { status: OUTBOX_STATUS.PUBLISHED, publishedAt: now },
    $unset: { lockedAt: 1, lockToken: 1, lastError: 1 },
  });
  return res.modifiedCount === 1;
}

/** Broker problem: give the row back untouched (attempts NOT incremented). */
export async function releaseForConnectivity(row, { error, retryInMs = 1000, now = new Date() } = {}) {
  const res = await Outbox.updateOne(fence(row), {
    $set: {
      status: OUTBOX_STATUS.PENDING,
      nextAttemptAt: new Date(now.getTime() + retryInMs),
      lastError: String(error?.message || error).slice(0, 500),
    },
    $unset: { lockedAt: 1, lockToken: 1 },
  });
  return res.modifiedCount === 1;
}

export function backoffMs(attempt, { baseMs = 1000, maxMs = 60_000, jitter = 0.2 } = {}) {
  const exp = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  const spread = exp * jitter;
  return Math.round(exp - spread + Math.random() * spread * 2);
}

/**
 * Row-attributable failure. permanent=true or attempts exhausted => FAILED, otherwise PENDING with backoff.
 * Returns { updated, status }.
 */
export async function markFailedAttempt(
  row,
  { error, permanent = false, maxAttempts = 5, now = new Date(), backoff = {} } = {}
) {
  const attempts = (row.attempts || 0) + 1;
  const lastError = String(error?.message || error).slice(0, 500);

  if (permanent || attempts >= maxAttempts) {
    const res = await Outbox.updateOne(fence(row), {
      $set: { status: OUTBOX_STATUS.FAILED, attempts, failedAt: now, lastError },
      $unset: { lockedAt: 1, lockToken: 1 },
    });
    return { updated: res.modifiedCount === 1, status: OUTBOX_STATUS.FAILED };
  }

  const res = await Outbox.updateOne(fence(row), {
    $set: {
      status: OUTBOX_STATUS.PENDING,
      attempts,
      lastError,
      nextAttemptAt: new Date(now.getTime() + backoffMs(attempts, backoff)),
    },
    $unset: { lockedAt: 1, lockToken: 1 },
  });
  return { updated: res.modifiedCount === 1, status: OUTBOX_STATUS.PENDING };
}

/** Put FAILED rows back in the queue (after the root cause was fixed). */
export async function requeueFailed({ eventIds, now = new Date() } = {}) {
  const filter = { status: OUTBOX_STATUS.FAILED };
  if (Array.isArray(eventIds) && eventIds.length > 0) filter.eventId = { $in: eventIds };
  const res = await Outbox.updateMany(filter, {
    $set: { status: OUTBOX_STATUS.PENDING, attempts: 0, nextAttemptAt: now },
    $unset: { failedAt: 1, lockedAt: 1, lockToken: 1 },
  });
  return res.modifiedCount;
}

export async function getOutboxStats(now = new Date()) {
  const [pending, processing, failed, oldestPending, lastFailed] = await Promise.all([
    Outbox.countDocuments({ status: OUTBOX_STATUS.PENDING }),
    Outbox.countDocuments({ status: OUTBOX_STATUS.PROCESSING }),
    Outbox.countDocuments({ status: OUTBOX_STATUS.FAILED }),
    Outbox.findOne({ status: OUTBOX_STATUS.PENDING }).sort({ nextAttemptAt: 1, createdAt: 1 }).select("createdAt").lean(),
    Outbox.findOne({ status: OUTBOX_STATUS.FAILED }).sort({ failedAt: -1 }).select("eventId lastError failedAt").lean(),
  ]);
  return {
    pending,
    processing,
    failed,
    oldestPendingAgeMs: oldestPending ? now.getTime() - new Date(oldestPending.createdAt).getTime() : 0,
    lastFailed: lastFailed || null,
  };
}