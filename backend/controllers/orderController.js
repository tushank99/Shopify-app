import { randomUUID } from "node:crypto";
import mongoose from "mongoose";
import { context, propagation } from "@opentelemetry/api";
import Order from "../models/orderModel.js";
import Product from "../models/productModel.js";
import { stockDecrementQueue } from "../queues/stockDecrementQueue.js";
import { mlRecommendationQueue } from "../queues/mlRecommendationQueue.js";
import { redisClient, reserveStockSha } from "../config/redis.js";
import { saveOrderWithOutbox } from "../outbox/orderOutbox.js"; // NEW

const OBJECT_ID_REGEX = /^[a-f\d]{24}$/i;

function calcPrices(orderItems) {
  const itemsPrice = orderItems.reduce((acc, item) => acc + item.price * item.qty, 0);
  const shippingPrice = 0;
  const taxPrice = 0;
  const totalPrice = itemsPrice;

  return {
    itemsPrice: itemsPrice.toFixed(2),
    shippingPrice: shippingPrice.toFixed(2),
    taxPrice: taxPrice.toFixed(2),
    totalPrice: totalPrice.toFixed(2),
  };
}

const createOrder = async (req, res) => {
  const reservedKeys = [];
  let orderCommitted = false;
  const session = await mongoose.startSession();

  try {
    const { orderItems, shippingAddress, paymentMethod } = req.body;

    if (!Array.isArray(orderItems) || orderItems.length === 0) {
      throw new Error("No order items");
    }

    const mergedClientItemsMap = new Map();
    for (const item of orderItems) {
      const qty = Number(item?.qty);
      if (
        !item ||
        typeof item._id !== "string" ||
        !OBJECT_ID_REGEX.test(item._id) ||
        !Number.isInteger(qty) ||
        qty < 1
      ) {
        throw new Error("InvalidOrderItem");
      }
      const existing = mergedClientItemsMap.get(item._id);
      if (existing) {
        existing.qty += qty;
      } else {
        mergedClientItemsMap.set(item._id, { ...item, qty });
      }
    }
    const normalizedOrderItems = [...mergedClientItemsMap.values()];

    const itemsFromDB = await Product.find({
      _id: { $in: normalizedOrderItems.map((x) => x._id) },
    });

    const dbOrderItems = normalizedOrderItems.map((itemFromClient) => {
      const matchingItemFromDB = itemsFromDB.find(
        (itemFromDB) => itemFromDB._id.toString() === itemFromClient._id
      );
      if (!matchingItemFromDB) {
        throw new Error(`ProductNotFound:${itemFromClient._id}`);
      }
      return {
        name: matchingItemFromDB.name,
        image: matchingItemFromDB.image,
        qty: itemFromClient.qty,
        product: matchingItemFromDB._id,
        price: matchingItemFromDB.price,
      };
    });

    for (const item of dbOrderItems) {
      const productIdStr = item.product.toString();
      const redisKey = `inventory:product:${productIdStr}`;

      let result = await redisClient.evalsha(reserveStockSha, 1, redisKey, item.qty);

      if (result === -2) {
        const productData = itemsFromDB.find((p) => p._id.toString() === productIdStr);
        await redisClient.set(redisKey, productData.countInStock, "EX", 86400, "NX");
        result = await redisClient.evalsha(reserveStockSha, 1, redisKey, item.qty);
      }

      if (result === -1) {
        throw new Error(`InsufficientStock:${productIdStr}`);
      }

      reservedKeys.push({ key: redisKey, qty: item.qty });
    }

    const { itemsPrice, taxPrice, shippingPrice, totalPrice } = calcPrices(dbOrderItems);

    const preGeneratedOrderId = new mongoose.Types.ObjectId();
    const paidAtDate = new Date();
    const mockPaymentId = "MOCK_PAYPAL_SANDBOX_ID_" + Math.random().toString(36).substring(2, 11);
    const userId = req.user._id.toString();

    // NEW: trace context of the HTTP request, replayed later by the relay (no-op without an OTel SDK).
    const traceCarrier = {};
    propagation.inject(context.active(), traceCarrier);

    // NEW: generated ONCE, outside the callback. withTransaction may re-run the callback,
    // but only one attempt commits, so one eventId <-> one committed order.
    const eventId = randomUUID();

    let createdOrder;

    await session.withTransaction(
      async () => {
        // Order + outbox row commit or roll back TOGETHER (same session).
        createdOrder = await saveOrderWithOutbox({
          session,
          eventId,
          traceCarrier,
          orderData: {
            _id: preGeneratedOrderId,
            orderItems: dbOrderItems,
            user: req.user._id,
            shippingAddress,
            paymentMethod,
            itemsPrice,
            taxPrice,
            shippingPrice,
            totalPrice,
            isPaid: true,
            paidAt: paidAtDate,
            paymentResult: {
              id: mockPaymentId,
              status: "COMPLETED",
              update_time: paidAtDate.toISOString(),
              email_address: req.user.email || "test_buyer@example.com",
            },
          },
        });
      },
      // An acknowledged order must survive a primary failover.
      { writeConcern: { w: "majority" } }
    );

    orderCommitted = true;

    const orderId = createdOrder._id.toString();

    const enqueueResults = await Promise.allSettled([
      stockDecrementQueue.add(
        "decrement-stock",
        {
          orderId,
          items: dbOrderItems.map((item) => ({
            productId: item.product.toString(),
            qty: item.qty,
          })),
        },
        { jobId: `stock-decrement-${orderId}` }
      ),
      mlRecommendationQueue.add(
        "update-recommendation",
        {
          userId,
          orderId,
          productIds: dbOrderItems.map((item) => item.product.toString()),
        },
        { jobId: `ml-recommendation-${orderId}` }
      ),
    ]);

    const enqueueLabels = ["stock-decrement", "ml-recommendation"];
    enqueueResults.forEach((result, i) => {
      if (result.status === "rejected") {
        console.error(
          `[CRITICAL]Order ${orderId}committed but ${enqueueLabels[i]}job was NOT enqueued: ${result.reason?.message}`
        );
      }
    });

    res.status(201).json(createdOrder);
  } catch (error) {
    if (reservedKeys.length > 0 && !orderCommitted) {
      try {
        const pipeline = redisClient.multi();
        for (const resItem of reservedKeys) {
          pipeline.incrby(resItem.key, resItem.qty);
        }
        await pipeline.exec();
      } catch (rollbackError) {
        console.error(
          `[CRITICAL]Failed to restore Redis stock reservations: ${rollbackError.message}`,
          reservedKeys
        );
      }
    }

    if (error.message === "No order items" || error.message === "InvalidOrderItem") {
      return res.status(400).json({ error: error.message });
    }

    if (error.message.startsWith("InsufficientStock")) {
      const productId = error.message.split(":")[1];
      return res.status(400).json({
        error: `Requested quantity exceeds available stock for product ${productId}`,
      });
    }

    if (error.message.startsWith("ProductNotFound")) {
      return res.status(404).json({ error: "One or more products not found in database" });
    }

    // NEW: an event that cannot be represented faithfully (bad price precision, contract violation)
    // aborts the whole transaction instead of silently writing a wrong ledger.
    if (error.message.startsWith("OutboxPayloadInvalid")) {
      console.error(`[CRITICAL]Order aborted, outbox payload invalid: ${error.message}`);
      return res.status(500).json({ error: "Order could not be recorded. Please contact support." });
    }

    res.status(500).json({ error: error.message });
  } finally {
    await session.endSession();
  }
};

const getAllOrders = async (req, res) => {
  try {
    const orders = await Order.find({}).populate("user", "id username");
    res.json(orders);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

const getUserOrders = async (req, res) => {
  try {
    const orders = await Order.find({ user: req.user._id });
    res.json(orders);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

const countTotalOrders = async (req, res) => {
  try {
    const totalOrders = await Order.countDocuments();
    res.json({ totalOrders });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

const calculateTotalSales = async (req, res) => {
  try {
    const orders = await Order.find();
    const totalSales = orders.reduce((sum, order) => sum + order.totalPrice, 0);
    res.json({ totalSales });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

const calcualteTotalSalesByDate = async (req, res) => {
  try {
    const salesByDate = await Order.aggregate([
      {
        $match: {
          isPaid: true,
        },
      },
      {
        $group: {
          _id: {
            $dateToString: { format: "%Y-%m-%d", date: "$paidAt" },
          },
          totalSales: { $sum: "$totalPrice" },
        },
      },
    ]);
    res.json(salesByDate);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

const findOrderById = async (req, res) => {
  try {
    const order = await Order.findById(req.params.id).populate("user", "username email");

    if (order) {
      res.json(order);
    } else {
      res.status(404);
      throw new Error("Order not found");
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

const markOrderAsPaid = async (req, res) => {
  try {
    const order = await Order.findById(req.params.id);

    if (order) {
      order.isPaid = true;
      order.paidAt = Date.now();
      order.paymentResult = {
        id: req.body.id,
        status: req.body.status,
        update_time: req.body.update_time,
        email_address: req.body.payer.email_address,
      };

      const updateOrder = await order.save();
      res.status(200).json(updateOrder);
    } else {
      res.status(404);
      throw new Error("Order not found");
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

const markOrderAsDelivered = async (req, res) => {
  try {
    const order = await Order.findById(req.params.id);

    if (order) {
      order.isDelivered = true;
      order.deliveredAt = Date.now();

      const updatedOrder = await order.save();
      res.json(updatedOrder);
    } else {
      res.status(404);
      throw new Error("Order not found");
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export {
  createOrder,
  getAllOrders,
  getUserOrders,
  countTotalOrders,
  calculateTotalSales,
  calcualteTotalSalesByDate,
  findOrderById,
  markOrderAsPaid,
  markOrderAsDelivered,
};