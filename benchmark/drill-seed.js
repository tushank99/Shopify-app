/**
 * Seeds what the failure drills need (safe to re-run):
 *   - DRILL_USERS users (@drill.test) with a signed JWT each (the API reads the JWT from the `jwt` COOKIE)
 *   - one product with a huge stock, so a drill never fails with InsufficientStock
 * Writes benchmark/drill_data.json, which benchmark/k6-drill-orders.js reads.
 *
 *   node benchmark/drill-seed.js
 *
 * Needs MONGO_URI and JWT_SECRET in the repo-root .env
 * (MONGO_URI=mongodb://127.0.0.1:27017/shopify?directConnection=true, see docker-compose.infra.yml).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import jwt from "jsonwebtoken";
import mongoose from "mongoose";
import User from "../backend/models/userModel.js";
import Product from "../backend/models/productModel.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, "../.env") });

const USERS = Number.parseInt(process.env.DRILL_USERS ?? "20", 10);
const STOCK = Number.parseInt(process.env.DRILL_STOCK ?? "100000", 10);
const PRODUCT_NAME = "DRILL PRODUCT (safe to delete)";
const PRICE = 49.99;

if (!process.env.MONGO_URI || !process.env.JWT_SECRET) {
  console.error("MONGO_URI and JWT_SECRET must be set in .env");
  process.exit(1);
}

try {
  await mongoose.connect(process.env.MONGO_URI, { serverSelectionTimeoutMS: 10_000 });

  // A fresh product every time => no stale Redis inventory key (the key is created from countInStock on first use).
  await Product.deleteMany({ name: PRODUCT_NAME });
  await User.deleteMany({ email: /@drill\.test$/ });

  const product = await Product.create({
    name: PRODUCT_NAME,
    image: "/uploads/drill.png",
    brand: "drill",
    quantity: STOCK,
    category: new mongoose.Types.ObjectId(),
    description: "Created by benchmark/drill-seed.js for the failure drills",
    price: PRICE,
    countInStock: STOCK,
  });

  const users = await User.insertMany(
    Array.from({ length: USERS }, (_, i) => ({
      username: `drilluser_${i}`,
      email: `drilluser_${i}@drill.test`,
      password: "drill-not-a-real-password",
      isAdmin: false,
    }))
  );

  const data = {
    productId: product._id.toString(),
    price: PRICE,
    users: users.map((u) => ({
      userId: u._id.toString(),
      token: jwt.sign({ userId: u._id.toString() }, process.env.JWT_SECRET, { expiresIn: "1d" }),
    })),
    createdAt: new Date().toISOString(),
  };

  fs.writeFileSync(path.join(__dirname, "drill_data.json"), JSON.stringify(data, null, 2));
  console.log(`Seeded ${users.length} users and product ${data.productId} (stock ${STOCK}, price ${PRICE}).`);
  console.log("Wrote benchmark/drill_data.json (contains JWTs: do not commit).");
  await mongoose.disconnect();
  process.exit(0);
} catch (err) {
  console.error(`Seeder error: ${err.message}`);
  process.exit(1);
}