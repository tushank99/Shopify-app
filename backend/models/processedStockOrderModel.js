import mongoose from "mongoose";

const processedStockOrderSchema = new mongoose.Schema({
  orderId: {
    type: mongoose.Schema.Types.ObjectId,
    required: true,
  },
  processedAt: {
    type: Date,
    required: true,
    default: Date.now,
  },
});

// The unique index IS the idempotency guarantee. Do not remove or relax it.
processedStockOrderSchema.index({ orderId: 1 }, { unique: true });

const ProcessedStockOrder = mongoose.model(
  "ProcessedStockOrder",
  processedStockOrderSchema
);

export default ProcessedStockOrder;