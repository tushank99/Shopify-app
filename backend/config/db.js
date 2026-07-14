import mongoose from "mongoose";

let isConnected = false;

const connectDB = async () => {
  if (isConnected) {
    console.log("Using existing MongoDB connection");
    return;
  }
  
  try {
    if (!process.env.MONGO_URI) {
      console.error("MONGO_URI environment variable is not set");
      return;
    }
    
    
    const conn = await mongoose.connect(process.env.MONGO_URI);

    isConnected = conn.connections[0].readyState === 1;
    console.log(`Successfully connected to MongoDB 👍`);
  } catch (error) {
    console.error(`MongoDB connection error: ${error.message}`);
   
    if (process.env.NODE_ENV !== "production") {
      process.exit(1);
    }
  }
};

export default connectDB;