import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Ajv from "ajv";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const ORDER_PLACED_TOPIC = "order.placed";
export const ORDER_PLACED_SCHEMA_VERSION = 1;

const SCHEMA_PATH =
  process.env.ORDER_PLACED_SCHEMA_PATH ||
  path.join(__dirname, "../../contracts/order.placed.v1.schema.json");

let validator = null;
function getValidator() {
  if (!validator) {
    const ajv = new Ajv({ allErrors: true, strict: true });
    validator = ajv.compile(JSON.parse(readFileSync(SCHEMA_PATH, "utf8")));
  }
  return validator;
}

/** Money is converted to integer minor units (paise/cents) so sums are exact. */
export function toMinorUnits(value, label) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) {
    throw new Error(`OutboxPayloadInvalid: ${label} is not a non-negative number (${value})`);
  }
  const minor = Math.round(n * 100);
  // More than 2 decimals would silently change the amount -> refuse rather than emit a wrong ledger.
  if (Math.abs(n * 100 - minor) > 1e-6) {
    throw new Error(`OutboxPayloadInvalid: ${label} has more than 2 decimal places (${value})`);
  }
  if (!Number.isSafeInteger(minor)) {
    throw new Error(`OutboxPayloadInvalid: ${label} is too large (${value})`);
  }
  return minor;
}

export function formatMinor(minor) {
  const whole = Math.trunc(minor / 100);
  const frac = String(minor % 100).padStart(2, "0");
  return `${whole}.${frac}`;
}

export function validateOrderPlaced(event) {
  const validate = getValidator();
  if (!validate(event)) {
    const detail = validate.errors.map((e) => `${e.instancePath || "/"} ${e.message}`).join("; ");
    throw new Error(`OutboxPayloadInvalid: ${detail}`);
  }
  return event;
}

/**
 * Builds the order.placed v1 payload from the order document of THIS transaction attempt.
 * Maps Mongo's qty/price -> contract quantity/unitPrice (2-decimal strings).
 */
export function buildOrderPlacedEvent({ eventId, order, currency }) {
  const cur = (currency || process.env.ORDER_CURRENCY || "INR").toUpperCase();

  let totalMinor = 0;
  const items = order.orderItems.map((line) => {
    const unitMinor = toMinorUnits(line.price, "item price");
    const quantity = Number(line.qty);
    if (!Number.isInteger(quantity) || quantity < 1) {
      throw new Error(`OutboxPayloadInvalid: item qty invalid (${line.qty})`);
    }
    totalMinor += unitMinor * quantity;
    if (!Number.isSafeInteger(totalMinor)) {
      throw new Error("OutboxPayloadInvalid: order total overflow");
    }
    return {
      productId: String(line.product),
      quantity,
      unitPrice: formatMinor(unitMinor),
    };
  });

  // Invariant: the event must agree with what the customer was charged.
  // (Today tax/shipping are 0 in calcPrices; when they are introduced, extend the contract.)
  const orderTotalMinor = toMinorUnits(order.totalPrice, "order totalPrice");
  if (orderTotalMinor !== totalMinor) {
    throw new Error(
      `OutboxPayloadInvalid: order total ${order.totalPrice} != sum of items ${formatMinor(totalMinor)}`
    );
  }

  const event = {
    eventId,
    eventType: ORDER_PLACED_TOPIC,
    schemaVersion: ORDER_PLACED_SCHEMA_VERSION,
    orderId: String(order._id),
    userId: String(order.user),
    items,
    totalPrice: formatMinor(totalMinor),
    currency: cur,
    createdAt: new Date(order.createdAt).toISOString(),
  };

  return validateOrderPlaced(event);
}