/**
 * Pure unit tests: no MongoDB, no Kafka. Safe to run anywhere (pnpm test:outbox runs them too).
 */
import { describe, it, expect } from "vitest";
import { isOutboxEnabled } from "../outbox/outboxConfig.js";
import {
  classifyError,
  needsProducerRecycle,
  SendTimeoutError,
  PayloadSerializationError,
} from "../outbox/kafkaErrors.js";

describe("isOutboxEnabled (gate on KAFKA_BROKERS)", () => {
  it("is off when KAFKA_BROKERS is missing, empty, blank or only commas", () => {
    expect(isOutboxEnabled({})).toBe(false);
    expect(isOutboxEnabled({ KAFKA_BROKERS: "" })).toBe(false);
    expect(isOutboxEnabled({ KAFKA_BROKERS: "   " })).toBe(false);
    expect(isOutboxEnabled({ KAFKA_BROKERS: " , ," })).toBe(false);
  });

  it("is on for one or several brokers", () => {
    expect(isOutboxEnabled({ KAFKA_BROKERS: "localhost:9092" })).toBe(true);
    expect(isOutboxEnabled({ KAFKA_BROKERS: "a:9092, b:9092" })).toBe(true);
  });

  it("OUTBOX_ENABLED=false is a hard off switch (any case, any padding)", () => {
    expect(isOutboxEnabled({ KAFKA_BROKERS: "localhost:9092", OUTBOX_ENABLED: "false" })).toBe(false);
    expect(isOutboxEnabled({ KAFKA_BROKERS: "localhost:9092", OUTBOX_ENABLED: " FALSE " })).toBe(false);
  });

  it("OUTBOX_ENABLED=true does NOT enable it without brokers", () => {
    expect(isOutboxEnabled({ OUTBOX_ENABLED: "true" })).toBe(false);
    expect(isOutboxEnabled({ KAFKA_BROKERS: "localhost:9092", OUTBOX_ENABLED: "true" })).toBe(true);
  });
});

describe("classifyError: plain KafkaJS messages during an outage are connectivity, not the row's fault", () => {
  const plain = (message, name = "KafkaJSError") => Object.assign(new Error(message), { name });

  it.each([
    "The producer is disconnected",
    "Closed connection",
    "Connection timeout",
    "Connection error: read ECONNRESET",
    "Failed to connect: connect ECONNREFUSED 127.0.0.1:9092",
    "socket hang up",
    "Broker not found",
    "Metadata not loaded",
    "Request timed out",
  ])("%s", (message) => {
    expect(classifyError(plain(message))).toBe("connectivity");
  });

  it("is still connectivity when wrapped by KafkaJSNumberOfRetriesExceeded", () => {
    const wrapped = { name: "KafkaJSNumberOfRetriesExceeded", message: "Retries exceeded", originalError: plain("The producer is disconnected") };
    expect(classifyError(wrapped)).toBe("connectivity");
  });

  it("existing classes and types are unchanged", () => {
    expect(classifyError(new SendTimeoutError(100))).toBe("connectivity");
    expect(classifyError({ type: "UNKNOWN_TOPIC_OR_PARTITION" })).toBe("connectivity");
    expect(classifyError({ type: "MESSAGE_TOO_LARGE" })).toBe("permanent");
    expect(classifyError(new PayloadSerializationError(new Error("circular")))).toBe("permanent");
  });

  it("does NOT over-match: genuine unknown errors still count toward FAILED", () => {
    expect(classifyError(new Error("boom"))).toBe("unknown");
    expect(classifyError(new Error("Unexpected token } in JSON"))).toBe("unknown");
    expect(classifyError(new Error("Invalid payload shape"))).toBe("unknown");
  });
});

describe("needsProducerRecycle", () => {
  it("true when the KafkaJS producer is dead (the relay's connected-flag would otherwise be stale forever)", () => {
    expect(needsProducerRecycle(new Error("The producer is disconnected"))).toBe(true);
    expect(needsProducerRecycle({ name: "KafkaJSNumberOfRetriesExceeded", originalError: new Error("The producer is not connected") })).toBe(true);
  });

  it("true for idempotent-producer state loss, false for ordinary outages", () => {
    expect(needsProducerRecycle({ type: "UNKNOWN_PRODUCER_ID" })).toBe(true);
    expect(needsProducerRecycle({ type: "OUT_OF_ORDER_SEQUENCE_NUMBER" })).toBe(true);
    expect(needsProducerRecycle(new Error("Connection timeout"))).toBe(false);
    expect(needsProducerRecycle({ type: "LEADER_NOT_AVAILABLE" })).toBe(false);
  });
});