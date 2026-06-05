import express from "express";
import formidable from "express-formidable";
const router = express.Router();

// controllers
import {
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
} from "../controllers/productController.js";
import { authenticate, authorizeAdmin } from "../middlewares/authMiddleware.js";
import checkId from "../middlewares/checkId.js";

router
  .route("/")
  .get(fetchProducts)
  .post(authenticate, authorizeAdmin, formidable(), addProduct);

// Specific routes must come BEFORE /:id to avoid being matched as IDs
router.route("/allproducts").get(fetchAllProducts);

router.get("/top", fetchTopProducts);
router.get("/new", fetchNewProducts);

router.get("/explain-query", explainQuery);
router.get("/test-mutex", testMutexLock);
router.route("/recommendations").get(authenticate, getRecommendations);
router.route("/filtered-products").post(filterProducts);


// Nested routes with IDs
router.route("/:id/reviews").post(authenticate, checkId, addProductReview);
router.route("/:id/reviews/helpful").post(authenticate, checkId, markReviewHelpful);
router.route("/:id/can-review").get(authenticate, checkId, canUserReview);

// Generic ID route comes last
router
  .route("/:id")
  .get(fetchProductById)
  .put(authenticate, authorizeAdmin, formidable(), updateProductDetails)
  .delete(authenticate, authorizeAdmin, removeProduct);

export default router;
