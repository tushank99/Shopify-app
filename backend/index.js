import path from "path";
import { fileURLToPath } from "url";
import express from "express";
import dotenv from "dotenv";
import cookieParser from "cookie-parser";
import cors from "cors";
import connectDB from "./config/db.js";
import userRoutes from "./routes/userRoutes.js";
import categoryRoutes from "./routes/categoryRoutes.js";
import productRoutes from "./routes/productRoutes.js";
import uploadRoutes from "./routes/uploadRoutes.js";
import orderRoutes from "./routes/orderRoutes.js";
import { notFound, errorHandler } from "./middlewares/errorMiddleware.js";
import "./queues/recommendationQueue.js";
import "./queues/orderQueue.js";
import queueRoutes from "./routes/queueRoutes.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.join(__dirname, "../.env") });

const port = process.env.PORT || 5000;

connectDB();

const app = express();
app.set('trust proxy', 1);
const allowedOrigins = [
  "http://localhost:5173",
  "http://localhost:3000",
  "https://shopify-app-zy9p.onrender.com",
  process.env.FRONTEND_URL,
].filter(Boolean);

const corsOptions = {
  origin: function (origin, callback) {
    if (!origin) return callback(null, true);
    if (allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error("Not allowed by CORS")); //  Now rejects unknown origins
    }
  },
  credentials: true,
  optionsSuccessStatus: 200
};

app.use(cors(corsOptions));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

app.use("/api/users", userRoutes);
app.use("/api/category", categoryRoutes);
app.use("/api/products", productRoutes);
app.use("/api/upload", uploadRoutes);
app.use("/api/orders", orderRoutes);
app.use("/api/queues", queueRoutes);

app.get("/api/config/paypal", (req, res) => {
  res.send({ clientId: process.env.PAYPAL_CLIENT_ID });
});

app.get("/api/health", (req, res) => {
  res.json({ status: "ok", message: "Server is running" });
});

app.get("/api/debug-db", async (req, res) => {
  try {
    const mongoose = (await import("mongoose")).default;
    const db = mongoose.connection.db;
    const dbName = db.databaseName;
    const collections = await db.listCollections().toArray();
    const collectionNames = collections.map((c) => c.name);
    const productCount = await db.collection("products").countDocuments();
    const categoryCount = await db.collection("categories").countDocuments();
    const userCount = await db.collection("users").countDocuments();
    const sampleProduct = await db.collection("products").findOne();

    res.json({
      connectedDatabase: dbName,
      
      mongoUri_masked: process.env.MONGO_URI ? process.env.MONGO_URI.replace(/\/\/[^@]+@/, "//***:***@") : "NOT SET",
      collections: collectionNames,
      counts: {
        products: productCount,
        categories: categoryCount,
        users: userCount,
      },
      sampleProduct: sampleProduct
        ? { name: sampleProduct.name, _id: sampleProduct._id }
        : null,
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.use("/uploads", express.static(path.join(__dirname + "/uploads")));

app.use("/api", notFound);
app.use("/api", errorHandler);

if (process.env.NODE_ENV === "production") {
  const frontendPath = path.join(__dirname, "../frontend/dist");
  app.use(express.static(frontendPath));
  app.get("*", (req, res) => {
    res.sendFile(path.join(frontendPath, "index.html"));
  });
}

app.listen(port, "0.0.0.0", () => {
  console.log(`Node Backend listening on 0.0.0.0:${port}`);
});

export default app;