import mongoose from "mongoose";
import Order from "../models/orderModel.js";
import Product from "../models/productModel.js";
import { orderQueue } from "../queues/orderQueue.js";
import { redisClient, reserveStockSha } from '../config/redis.js';

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
  // Array to track successful memory reservations for rollback if needed
  const reservedKeys = [];
  const session = await mongoose.startSession();

  try {
    const { orderItems, shippingAddress, paymentMethod } = req.body;

    if (orderItems && orderItems.length === 0) {
      res.status(400);
      throw new Error("No order items");
    }

    // ---  Secure Price Validation ---
    // Fetch products from DB to ensure clients can't spoof prices
    const itemsFromDB = await Product.find({
      _id: { $in: orderItems.map((x) => x._id) },
    });

    const dbOrderItems = orderItems.map((itemFromClient) => {
      const matchingItemFromDB = itemsFromDB.find(
        (itemFromDB) => itemFromDB._id.toString() === itemFromClient._id
      );
      if (!matchingItemFromDB) {
        throw new Error(`ProductNotFound:${itemFromClient._id}`);
      }
      return {
        ...itemFromClient,
        product: itemFromClient._id,
        price: matchingItemFromDB.price,
        _id: undefined,
      };
    });

    // ---  Atomic Redis Inventory Reservation ---
    for (const item of dbOrderItems) {
      const redisKey = `inventory:product:${item.product}`;

      // Attempt reservation via cached Lua script
      let result = await redisClient.evalsha(reserveStockSha, 1, redisKey, item.qty);

      // Hydration Gate (Result -2)
      if (result === -2) {
        const productData = itemsFromDB.find((p) => p._id.toString() === item.product);
        
        // Set in Redis with a 24-hour expiration
        await redisClient.set(redisKey, productData.countInStock, 'EX', 86400);
        
        // Re-run the script
        result = await redisClient.evalsha(reserveStockSha, 1, redisKey, item.qty);
      }

      // Stock Validation Gate (Result -1)
      if (result === -1) {
        throw new Error(`InsufficientStock:${item.product}`);
      }

      // Success: Track this specific item so we can roll it back if the DB crashes
      reservedKeys.push({ key: redisKey, qty: item.qty });
    }

    // ---  Finalize Order & Database Write ---
    const { itemsPrice, taxPrice, shippingPrice, totalPrice } = calcPrices(dbOrderItems);
    let createdOrder;
    await session.withTransaction(async () => {
      const order = new Order({
      orderItems: dbOrderItems,
      user: req.user._id,
      shippingAddress,
      paymentMethod,
      itemsPrice,
      taxPrice,
      shippingPrice,
      totalPrice,
      isPaid: true,
      paidAt: Date.now(),
      paymentResult: {
        id: "MOCK_PAYPAL_SANDBOX_ID_" + Math.random().toString(36).substr(2, 9),
        status: "COMPLETED",
        update_time: new Date().toISOString(),
        email_address: req.user.email || "test_buyer@example.com",
      },
    });
    const savedOrders = await order.save({ session });
    createdOrder = savedOrders;

    });
    
    await orderQueue.add(
      `order-placed-${createdOrder._id}`,
      {
        userId: req.user._id.toString(),
        orderId: createdOrder._id.toString(),
        items: dbOrderItems,
      },
      { attempts: 3, backoff: 5000 }
    );

    res.status(201).json(createdOrder);

  } catch (error) {
    // ---  Compensation & Rollback ---
    // If the DB crashes, or stock is insufficient, restore any reserved items back to Redis
    if (reservedKeys.length > 0) {
      const pipeline = redisClient.multi();
      for (const resItem of reservedKeys) {
        pipeline.incrby(resItem.key, resItem.qty);
      }
      await pipeline.exec();
    }

    // Handle custom thrown errors gracefully
    if (error.message.startsWith('InsufficientStock')) {
      const productId = error.message.split(':')[1];
      return res.status(400).json({ error: `Requested quantity exceeds available stock for product ${productId}` });
    }
    if (error.message.startsWith('ProductNotFound')) {
      return res.status(404).json({ error: "One or more products not found in database" });
    }

    res.status(500).json({ error: error.message });
    
  }
  finally {
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
    const order = await Order.findById(req.params.id).populate(
      "user",
      "username email"
    );
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