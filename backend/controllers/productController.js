import asyncHandler from "../middlewares/asyncHandler.js";
import Product from "../models/productModel.js";
import Order from "../models/orderModel.js";
import axios from "axios";
import redis from "../config/redis.js";
import { recommendationQueue } from "../queues/recommendationQueue.js";

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
    const pageSize = parseInt(req.query.limit) || 50;
    const page = parseInt(req.query.page) || 1;
    
    // Switch from slow $regex to optimized native MongoDB Text Index search
    let query = {};
    if (req.query.keyword) {
      query = { $text: { $search: req.query.keyword } };
    }

    const count = await Product.countDocuments(query);
    
    // If performing a text search, sort by text relevance score; otherwise sort by newest
    let products;
    if (req.query.keyword) {
      products = await Product.find(query)
        .select({ score: { $meta: "textScore" } })
        .sort({ score: { $meta: "textScore" } })
        .limit(pageSize)
        .skip(pageSize * (page - 1));
    } else {
      products = await Product.find(query)
        .sort({ createdAt: -1 }) // Utilizes our new { createdAt: -1 } B-Tree Index!
        .limit(pageSize)
        .skip(pageSize * (page - 1))
        .populate("category");
    }

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

      // DISPATCH THE EVENT DIRECTLY TO THE EVENT BUFFER QUEUE
      await recommendationQueue.add(`review-submitted-${req.user._id}-${product._id}`, {
        userId: req.user._id.toString(),
        productId: product._id.toString(),
        rating: Number(rating)
      }, {
        attempts: 3,
        backoff: 5000
      });

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
  const userId = req.user._id.toString();
  const cacheKey = `recs:${userId}`;

  const handleFallback = async () => {
    return await Product.find({}).populate("category").sort({ rating: -1 }).limit(12);
  };

  try {
    // 1. Check if recommendations exist in the Redis RAM Cache
    const cachedData = await redis.get(cacheKey);
    if (cachedData) {
      console.log(`Redis Cache Hit: Serving high-speed recommendations for user: ${userId}`);
      return res.json(JSON.parse(cachedData));
    }

    console.log(`Redis Cache Miss: Fetching fresh data for user: ${userId}`);

    // 2. Cold Start Check: See if the user has left any reviews
    const hasReviews = await Product.findOne({ "reviews.user": req.user._id });
    if (!hasReviews) {
      console.log(`Cold Start: User ${userId} has no history. Serving top-rated fallback.`);
      const fallbackProducts = await handleFallback();
      
      // Cache the cold start fallback for 24 hours so we don't query MongoDB constantly
      await redis.set(cacheKey, JSON.stringify(fallbackProducts), "EX", 86400);
      return res.json(fallbackProducts);
    }

    // 3. Dispatch recommendation request to Python ML sidecar
    console.log(`Dispatched recommendation request to ML sidecar for user: ${userId}`);
    const response = await axios.get(`http://127.0.0.1:8000/recommend/${userId}`, { timeout: 1000 });
    const recommendedIds = response.data.recommendations;

    if (!recommendedIds || recommendedIds.length === 0) {
      const fallbackProducts = await handleFallback();
      await redis.set(cacheKey, JSON.stringify(fallbackProducts), "EX", 86400);
      return res.json(fallbackProducts);
    }

    // 4. Fetch the predicted products from MongoDB and maintain Python's strict SVD order
    const products = await Product.find({ _id: { $in: recommendedIds } }).populate("category");
    const orderedProducts = recommendedIds
      .map(id => products.find(prod => prod._id.toString() === id))
      .filter(Boolean);

    // 5. Save the ordered array into Redis with a 24-Hour Expiration (86400 seconds)
    await redis.set(cacheKey, JSON.stringify(orderedProducts), "EX", 86400);

    res.json(orderedProducts);
  } catch (error) {
    console.error(`ML Engine integration failed: ${error.message}. Executing resilient fallback layer.`);
    const fallbackProducts = await handleFallback();
    res.json(fallbackProducts);
  }
});
const explainQuery = asyncHandler(async (req, res) => {
  try {
    const keyword = req.query.keyword || "";
    let query = {};
    
    if (keyword) {
      query = { $text: { $search: keyword } };
    }

    // Execute the query planner details directly from MongoDB
    const executionStats = await Product.find(query)
      .sort(keyword ? {} : { createdAt: -1 })
      .explain("executionStats");

    res.json({
      message: "Query optimization diagnostic report generated successfully.",
      targetQuery: query,
      winningPlanStage: executionStats.queryPlanner?.winningPlan?.stage || "UNKNOWN",
      stats: executionStats.executionStats
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});
const testMutexLock = asyncHandler(async (req, res) => {
  const testLockKey = "recs:lock:test_user_123";

  try {
    // Attempt to acquire the lock for 5 seconds
    const acquiredLock = await redis.set(testLockKey, "locked", "EX", 5, "NX");

    if (!acquiredLock) {
      //  Subsequent requests hit this block instantly
      return res.status(429).json({ 
        message: "Cache Stampede Prevented! Mutex is locked. Serving fallback data." 
      });
    }

    // 1st request gets here. Simulate a 2-second heavy Python ML calculation delay
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Release lock
    await redis.del(testLockKey);

    return res.status(200).json({ 
      message: "Success! Request 1 acquired the lock and completed the heavy calculation." 
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
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
  explainQuery,
  testMutexLock,
};
