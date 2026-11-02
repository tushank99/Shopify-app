/**
 * Single source of truth for "should this process write outbox rows?".
 *
 * The outbox is only useful when a relay + Kafka exist. On a deployment without them
 * (for example a plain API host) PENDING rows would pile up forever, because the TTL index
 * only removes PUBLISHED rows. So the outbox write is on only when KAFKA_BROKERS is set.
 *
 *   OUTBOX_ENABLED=false  -> hard off switch, even when KAFKA_BROKERS is set.
 *
 * Note: orders placed while the outbox is off never produce an event (there is no backfill).
 */
export function isOutboxEnabled(env = process.env) {
  if (String(env.OUTBOX_ENABLED ?? "").trim().toLowerCase() === "false") return false;
  return String(env.KAFKA_BROKERS ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean).length > 0;
}