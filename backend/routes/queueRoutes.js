import express from "express";
import { retryFailedJobs, outboxStats, requeueFailedOutbox } from "../controllers/queueController.js";
import { authenticate, authorizeAdmin } from "../middlewares/authMiddleware.js";

const router = express.Router();

router.route("/retry-failed").post(authenticate, authorizeAdmin, retryFailedJobs);

// NEW: transactional-outbox administration
router.route("/outbox/stats").get(authenticate, authorizeAdmin, outboxStats);
router.route("/outbox/requeue-failed").post(authenticate, authorizeAdmin, requeueFailedOutbox);

export default router;