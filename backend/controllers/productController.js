import asyncHandler from "../middlewares/asyncHandler.js";
import Product from "../models/productModel.js";
import Order from "../models/orderModel.js";
import axios from "axios";

const addProduct = asyncHandler(async (req, res) => {
  try {
    const { name, description, price, category, quantity, brand } = req.fields;

    // Validation
    switch (true) {
      case !name:
        return res.json({ error: "Name is required" });
      case !brand:
        return res.json({ error: "Brand is required" });
      case !description:
        return res.json({ error: "Description is required" });
      case !price:
        return res.json({ error: "Price is required" });
      case !category:
        return res.json({ error: "Category is required" });
      case !quantity:
        return res.json({ error: "Quantity is required" });
    }

    const product = new Product({ ...req.fields });
    await product.save();
    res.json(product);
  } catch (error) {
    console.error(error);
    res.status(400).json(error.message);
  }
});

const updateProductDetails = asyncHandler(async (req, res) => {
  try {
    const { name, description, price, category, quantity, brand } = req.fields;

    // Validation
    switch (true) {
      case !name:
        return res.json({ error: "Name is required" });
      case !brand:
        return res.json({ error: "Brand is required" });
      case !description:
        return res.json({ error: "Description is required" });
      case !price:
        return res.json({ error: "Price is required" });
      case !category:
        return res.json({ error: "Category is required" });
      case !quantity:
        return res.json({ error: "Quantity is required" });
    }

    const product = await Product.findByIdAndUpdate(
      req.params.id,
      { ...req.fields },
      { new: true }
    );

    await product.save();

    res.json(product);
  } catch (error) {
    console.error(error);
    res.status(400).json(error.message);
  }
});

const removeProduct = asyncHandler(async (req, res) => {
  try {
    const product = await Product.findByIdAndDelete(req.params.id);
    res.json(product);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Server error" });
  }
});

const fetchProducts = asyncHandler(async (req, res) => {
  try {
    const pageSize = parseInt(req.query.limit) || 50; // Increased default limit
    const page = parseInt(req.query.page) || 1;

    const keyword = req.query.keyword
      ? {
          $or: [
            { name: { $regex: req.query.keyword, $options: "i" } },
            { brand: { $regex: req.query.keyword, $options: "i" } },
            { description: { $regex: req.query.keyword, $options: "i" } },
          ],
        }
      : {};

    const count = await Product.countDocuments({ ...keyword });
    const products = await Product.find({ ...keyword })
      .limit(pageSize)
      .skip(pageSize * (page - 1))
      .sort({ createdAt: -1 });

    res.json({
      products,
      page,
      pages: Math.ceil(count / pageSize),
      total: count,
      hasMore: page < Math.ceil(count / pageSize),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Server Error" });
  }
});

const fetchProductById = asyncHandler(async (req, res) => {
  try {
    const product = await Product.findById(req.params.id);
    if (product) {
      return res.json(product);
    } else {
      res.status(404);
      throw new Error("Product not found");
    }
  } catch (error) {
    console.error(error);
    res.status(404).json({ error: "Product not found" });
  }
});

const fetchAllProducts = asyncHandler(async (req, res) => {
  try {
    const products = await Product.find({})
      .populate("category")
      .limit(12)
      .sort({ createAt: -1 });

    res.json(products);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Server Error" });
  }
});

const addProductReview = asyncHandler(async (req, res) => {
  try {
    const { rating, comment, title } = req.body;
    const product = await Product.findById(req.params.id);

    if (product) {
      const alreadyReviewed = product.reviews.find(
        (r) => r.user && r.user.toString() === req.user._id.toString()
      );

      if (alreadyReviewed) {
        res.status(400);
        throw new Error("You have already reviewed this product");
      }

      // Check if user has purchased this product
      const hasPurchased = await Order.findOne({
        user: req.user._id,
        "orderItems.product": req.params.id,
        isPaid: true,
      });

      if (!hasPurchased) {
        res.status(400);
        throw new Error("You can only review products you have purchased");
      }

      const review = {
        name: req.user.username,
        rating: Number(rating),
        comment,
        title: title || "",
        user: req.user._id,
        isVerifiedPurchase: true,
        helpfulVotes: 0,
      };

      product.reviews.push(review);

      product.numReviews = product.reviews.length;

      product.rating =
        product.reviews.reduce((acc, item) => item.rating + acc, 0) /
        product.reviews.length;

      await product.save();
      res.status(201).json({ message: "Review added successfully" });
    } else {
      res.status(404);
      throw new Error("Product not found");
    }
  } catch (error) {
    console.error(error);
    res.status(400).json(error.message);
  }
});

// Mark review as helpful
const markReviewHelpful = asyncHandler(async (req, res) => {
  try {
    const { reviewId } = req.body;
    const product = await Product.findById(req.params.id);

    if (product) {
      const review = product.reviews.id(reviewId);
      if (review) {
        review.helpfulVotes += 1;
        await product.save();
        res.json({ message: "Marked as helpful", helpfulVotes: review.helpfulVotes });
      } else {
        res.status(404);
        throw new Error("Review not found");
      }
    } else {
      res.status(404);
      throw new Error("Product not found");
    }
  } catch (error) {
    console.error(error);
    res.status(400).json(error.message);
  }
});

// Check if user can review a product
const canUserReview = asyncHandler(async (req, res) => {
  try {
    const product = await Product.findById(req.params.id);
    
    if (!product) {
      return res.status(404).json({ canReview: false, message: "Product not found" });
    }

    // Check if already reviewed
    const alreadyReviewed = product.reviews.find(
      (r) => r.user && r.user.toString() === req.user._id.toString()
    );

    if (alreadyReviewed) {
      return res.json({ canReview: false, message: "You have already reviewed this product" });
    }

    // Check if user has purchased this product
    const hasPurchased = await Order.findOne({
      user: req.user._id,
      "orderItems.product": req.params.id,
      isPaid: true,
    });

    if (!hasPurchased) {
      return res.json({ canReview: false, message: "Purchase this product to write a review" });
    }

    res.json({ canReview: true, message: "You can review this product" });
  } catch (error) {
    console.error(error);
    res.status(400).json({ canReview: false, message: error.message });
  }
});

const fetchTopProducts = asyncHandler(async (req, res) => {
  try {
    const products = await Product.find({}).sort({ rating: -1 }).limit(4);
    res.json(products);
  } catch (error) {
    console.error(error);
    res.status(400).json(error.message);
  }
});

const fetchNewProducts = asyncHandler(async (req, res) => {
  try {
    const products = await Product.find().sort({ _id: -1 }).limit(5);
    res.json(products);
  } catch (error) {
    console.error(error);
    res.status(400).json(error.message);
  }
});

const filterProducts = asyncHandler(async (req, res) => {
  try {
    const { checked, radio } = req.body;

    let args = {};
    if (checked.length > 0) args.category = checked;
    if (radio.length) args.price = { $gte: radio[0], $lte: radio[1] };

    const products = await Product.find(args);
    res.json(products);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Server Error" });
  }
});

// @desc    Get personalized SVD recommendations for a logged-in user via Python microservice
// @route   GET /api/products/recommendations
// @access  Private
const getRecommendations = asyncHandler(async (req, res) => {
  // Define fallback logic clearly so we can use it in multiple places if something breaks
  const handleFallback = async () => {
    return await Product.find({})
      .populate("category")
      .sort({ rating: -1 })
      .limit(12); // Send a strong selection of 12 top-tier items
  };

  try {
    const userId = req.user._id.toString();

    // 1. Defend against the Cold Start Problem
    // If the user hasn't dropped a review yet, don't waste network cycles calling Python
    const hasReviews = await Product.findOne({ "reviews.user": req.user._id });

    if (!hasReviews) {
      console.log(`❄️ Cold Start: User ${userId} has no transaction history. Serving top-rated fallback.`);
      const fallbackProducts = await handleFallback();
      return res.json(fallbackProducts);
    }

    // 2. Dispatch a low-latency HTTP GET request to our Python FastAPI service
    console.log(`📡 Dispatched recommendation request to ML sidecar for user: ${userId}`);
    
    // We explicitly assign a tight 1-second timeout so a hanging ML service won't freeze our Node app
    const response = await axios.get(`http://127.0.0.1:8000/recommend/${userId}`, { timeout: 1000 });
    const recommendedIds = response.data.recommendations;

    if (!recommendedIds || recommendedIds.length === 0) {
      const fallbackProducts = await handleFallback();
      return res.json(fallbackProducts);
    }

    // 3. Query MongoDB for the full product documents matching these specific IDs
    const products = await Product.find({ _id: { $in: recommendedIds } }).populate("category");

    // CRITICAL MATCHING STEP: MongoDB's $in operator shuffles the data order.
    // We map over our explicit recommendedIds array to guarantee our final JSON array
    // perfectly mirrors the mathematical ranking order calculated by SVD.
    const orderedProducts = recommendedIds
      .map(id => products.find(prod => prod._id.toString() === id))
      .filter(Boolean); // Discard any undefined values safely

    res.json(orderedProducts);

  } catch (error) {
    // 4. Fault Tolerance: If Python crashes, timed out, or threw an error, degrade gracefully
    console.error(`⚠️ ML Engine integration failed: ${error.message}. Executing resilient fallback layer.`);
    const fallbackProducts = await handleFallback();
    res.json(fallbackProducts);
  }
});


export {
  addProduct,
  updateProductDetails,
  removeProduct,
  fetchProducts,
  fetchProductById,
  fetchAllProducts,
  addProductReview,
  fetchTopProducts,
  fetchNewProducts,
  filterProducts,
  markReviewHelpful,
  canUserReview,
  getRecommendations,
};
