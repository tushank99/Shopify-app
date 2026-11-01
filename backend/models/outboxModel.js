import mongoose from "mongoose";

export const OUTBOX_STATUS = Object.freeze({
  PENDING: "PENDING",
  PROCESSING: "PROCESSING",
  PUBLISHED: "PUBLISHED",
  FAILED: "FAILED",
});

const outboxSchema = new mongoose.Schema(
  {
    // UUIDv4 generated in createOrder. Unique => a retried transaction callback can never double-insert.
    eventId: { type: String, required: true, unique: true },
    topic: { type: String, required: true },
    partitionKey: { type: String, required: true },
    payload: { type: mongoose.Schema.Types.Mixed, required: true },

    // W3C trace context captured at order time (empty object when no OTel SDK is registered).
    traceCarrier: {
      traceparent: { type: String },
      tracestate: { type: String },
    },

    status: {
      type: String,
      enum: Object.values(OUTBOX_STATUS),
      default: OUTBOX_STATUS.PENDING,
      required: true,
    },

    // Counts only attempts that are the row's "fault" (unknown errors). Broker outages never count.
    attempts: { type: Number, default: 0 },

    // MUST default to "now": a missing field never matches { nextAttemptAt: { $lte: now } }.
    nextAttemptAt: { type: Date, default: Date.now, required: true },

    // Lease + fencing token. lockToken is regenerated on every claim; the final
    // PUBLISHED/retry/FAILED update is conditional on (status=PROCESSING, lockToken).
    lockedAt: { type: Date },
    lockToken: { type: String },

    publishedAt: { type: Date },
    failedAt: { type: Date },
    lastError: { type: String },
  },
  { timestamps: true } // createdAt is used as the tie-breaker in the claim sort
);

// Claim path 1: pending rows ready to send, oldest-due first (no in-memory sort).
outboxSchema.index({ status: 1, nextAttemptAt: 1, createdAt: 1 }, { name: "claim_pending" });
// Claim path 2: reclaim expired leases (sorted by lockedAt).
outboxSchema.index({ status: 1, lockedAt: 1 }, { name: "claim_expired_lease" });
// Only successfully published rows carry publishedAt, so only they expire (7 days).
outboxSchema.index({ publishedAt: 1 }, { name: "ttl_published", expireAfterSeconds: 604800 });

const Outbox = mongoose.models.Outbox || mongoose.model("Outbox", outboxSchema);

export default Outbox;