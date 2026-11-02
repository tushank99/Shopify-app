import Order from "../models/orderModel.js";
import Outbox, { OUTBOX_STATUS } from "../models/outboxModel.js";
import { buildOrderPlacedEvent, ORDER_PLACED_TOPIC } from "../events/orderPlaced.js";
import { isOutboxEnabled } from "./outboxConfig.js";

let warnedDisabled = false;

/**
 * Persists the order and its outbox row in ONE Mongo transaction.
 * Must be called inside session.withTransaction(). The payload is rebuilt from the order
 * saved by THIS attempt, so a transparent transaction retry never reuses stale data.
 *
 * When the outbox is disabled (no KAFKA_BROKERS, or OUTBOX_ENABLED=false) only the order is saved:
 * no event is built, so a contract problem cannot block checkout on a deployment that has no relay.
 */
export async function saveOrderWithOutbox({
  session,
  orderData,
  eventId,
  traceCarrier,
  outboxEnabled = isOutboxEnabled(),
}) {
  const order = new Order(orderData);
  const savedOrder = await order.save({ session });

  if (!outboxEnabled) {
    if (!warnedDisabled) {
      warnedDisabled = true;
      console.warn(
        "[outbox] disabled (KAFKA_BROKERS is not set or OUTBOX_ENABLED=false): orders are saved without an outbox row"
      );
    }
    return savedOrder;
  }

  const payload = buildOrderPlacedEvent({ eventId, order: savedOrder });

  await Outbox.create(
    [
      {
        eventId,
        topic: ORDER_PLACED_TOPIC,
        partitionKey: String(savedOrder.user),
        payload,
        traceCarrier: traceCarrier || {},
        status: OUTBOX_STATUS.PENDING,
        nextAttemptAt: new Date(),
      },
    ],
    { session }
  );

  return savedOrder;
}