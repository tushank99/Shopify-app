import express from 'express';
import { retryFailedJobs } from '../controllers/queueController.js';
import { authenticate, authorizeAdmin } from '../middlewares/authMiddleware.js';

const router = express.Router();

router.route('/retry-failed').post(authenticate, authorizeAdmin, retryFailedJobs);

export default router;