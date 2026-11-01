import Order from "../models/orderModel.js";
import Outbox, { OUTBOX_STATUS } from "../models/outboxModel.js";
import { buildOrderPlacedEvent, ORDER_PLACED_TOPIC } from "../events/orderPlaced.js";

/**
 * Persists the order and its outbox row in ONE Mongo transaction.
 * Must be called inside session.withTransaction(). The payload is rebuilt from the order
 * saved by THIS attempt, so a transparent transaction retry never reuses stale data.
 */
export async function saveOrderWithOutbox({ session, orderData, eventId, traceCarrier }) {
  const order = new Order(orderData);
  const savedOrder = await order.save({ session });

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