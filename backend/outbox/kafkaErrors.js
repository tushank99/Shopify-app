/**
 * Classifies a publish failure so the relay can react correctly:
 *
 *  - "connectivity": broker/topic/leader problem. NOT the row's fault. Never counts toward FAILED;
 *                    the relay closes its broker gate and stops claiming until the broker answers.
 *  - "permanent":    the row itself can never be sent (oversized/invalid record, unserialisable payload).
 *                    Goes straight to FAILED.
 *  - "unknown":      anything else. Retried with backoff and counted; FAILED after max attempts.
 */

export class SendTimeoutError extends Error {
  constructor(ms) {
    super(`Kafka send exceeded ${ms}ms`);
    this.name = "SendTimeoutError";
  }
}

export class PayloadSerializationError extends Error {
  constructor(cause) {
    super(`Outbox payload could not be serialised: ${cause?.message}`);
    this.name = "PayloadSerializationError";
    this.cause = cause;
  }
}

const PERMANENT_TYPES = new Set([
  "MESSAGE_TOO_LARGE",
  "RECORD_LIST_TOO_LARGE",
  "INVALID_RECORD",
  "CORRUPT_MESSAGE",
  "INVALID_MESSAGE_SIZE",
]);

// UNKNOWN_TOPIC_OR_PARTITION is environmental here (auto-create is off, topic may not exist yet),
// so it drains once the topic is created instead of burning attempts.
const CONNECTIVITY_TYPES = new Set([
  "LEADER_NOT_AVAILABLE",
  "NOT_LEADER_FOR_PARTITION",
  "NOT_LEADER_OR_FOLLOWER",
  "UNKNOWN_TOPIC_OR_PARTITION",
  "REQUEST_TIMED_OUT",
  "NETWORK_EXCEPTION",
  "BROKER_NOT_AVAILABLE",
  "COORDINATOR_NOT_AVAILABLE",
  "NOT_COORDINATOR",
  "NOT_ENOUGH_REPLICAS",
  "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
  "TOPIC_AUTHORIZATION_FAILED",
  "CLUSTER_AUTHORIZATION_FAILED",
  // idempotent-producer state loss after a broker restart: recreate the producer, don't blame the row
  "OUT_OF_ORDER_SEQUENCE_NUMBER",
  "UNKNOWN_PRODUCER_ID",
  "INVALID_PRODUCER_EPOCH",
  "PRODUCER_FENCED",
]);

const CONNECTIVITY_NAMES =
  /^(KafkaJSConnectionError|KafkaJSConnectionClosedError|KafkaJSBrokerNotFound|KafkaJSRequestTimeoutError|KafkaJSTimeout|KafkaJSMetadataNotLoaded|SendTimeoutError)$/;
const CONNECTIVITY_CODES = /ECONNREFUSED|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|EPIPE|EHOSTUNREACH|ENETUNREACH/;

/** Walks .originalError / .cause so KafkaJSNumberOfRetriesExceeded wrappers are unwrapped. */
function errorChain(err) {
  const chain = [];
  const seen = new Set();
  let cur = err;
  while (cur && typeof cur === "object" && !seen.has(cur) && chain.length < 10) {
    seen.add(cur);
    chain.push(cur);
    cur = cur.originalError || cur.cause;
  }
  return chain;
}

export function classifyError(err) {
  const chain = errorChain(err);

  for (const e of chain) {
    if (e instanceof PayloadSerializationError) return "permanent";
    if (e?.type && PERMANENT_TYPES.has(e.type)) return "permanent";
  }
  for (const e of chain) {
    if (e?.type && CONNECTIVITY_TYPES.has(e.type)) return "connectivity";
    if (e?.name && CONNECTIVITY_NAMES.test(e.name)) return "connectivity";
    if (e?.code && CONNECTIVITY_CODES.test(String(e.code))) return "connectivity";
    if (e?.message && CONNECTIVITY_CODES.test(String(e.message))) return "connectivity";
  }
  return "unknown";
}

/** Types after which the (idempotent) producer should be rebuilt on recovery. */
export function needsProducerRecycle(err) {
  return errorChain(err).some((e) =>
    ["OUT_OF_ORDER_SEQUENCE_NUMBER", "UNKNOWN_PRODUCER_ID", "INVALID_PRODUCER_EPOCH", "PRODUCER_FENCED"].includes(e?.type)
  );
}