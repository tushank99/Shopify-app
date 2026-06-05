import mongoose from "mongoose";
const { ObjectId } = mongoose.Schema;

const reviewSchema = mongoose.Schema(
  {
    name: { type: String, required: true },
    rating: { type: Number, required: true },
    comment: { type: String, required: true },
    title: { type: String, default: "" },
    user: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
    },
    isVerifiedPurchase: { type: Boolean, default: false },
    helpfulVotes: { type: Number, default: 0 },
    images: [{ type: String }],
  },
  { timestamps: true }
);

const productSchema = mongoose.Schema(
  {
    name: { type: String, required: true },
    image: { type: String, required: true },
    brand: { type: String, required: true },
    quantity: { type: Number, required: true },
    category: { type: ObjectId, ref: "Category", required: true },
    description: { type: String, required: true },
    reviews: [reviewSchema],
    rating: { type: Number, required: true, default: 0 },
    numReviews: { type: Number, required: true, default: 0 },
    price: { type: Number, required: true, default: 0 },
    countInStock: { type: Number, required: true, default: 0 },
  },
  { timestamps: true }
);

const Product = mongoose.model("Product", productSchema);

// Compound Index for Category filtering and Price ranges
productSchema.index({ category: 1, price: 1 });

// Sorting Index to eliminate in-memory sorting overhead
productSchema.index({ createdAt: -1 });

// Enterprise Text Index across the catalog fields for the $text engine
productSchema.index(
  { name: "text", description: "text", brand: "text" },
  { weights: { name: 10, brand: 5, description: 1 } } // 'name' matches score higher!
);
export default Product;
