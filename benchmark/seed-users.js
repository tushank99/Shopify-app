import mongoose from 'mongoose';
import jwt from 'jsonwebtoken';
import fs from 'fs';
import dotenv from 'dotenv';
import User from '../backend/models/userModel.js';

dotenv.config({ path: '../.env' }); 

const generateToken = (id) => {
  return jwt.sign({ userId: id }, process.env.JWT_SECRET, { expiresIn: '30d' });
};

const seedUsers = async () => {
  try {
    await mongoose.connect(process.env.MONGO_URI);
    console.log('Connected to MongoDB. Starting data generation...');

    await User.deleteMany({ email: { $regex: 'mockuser' } });

    const tokens = [];
    const usersToInsert = [];

    console.log('Building 500 valid mock users...');
    for (let i = 0; i < 500; i++) {
      usersToInsert.push({
        username: `mockuser_${i}`,
        email: `mockuser_${i}@test.com`,
        password: 'password123', 
        isAdmin: false,
      });
    }

    const insertedUsers = await User.insertMany(usersToInsert);

    // Generate valid JWTs for every inserted user
    insertedUsers.forEach((user) => {
      tokens.push(generateToken(user._id));
    });

    // Write to valid_tokens.json in the same benchmark directory
    fs.writeFileSync('./benchmark/valid_tokens.json', JSON.stringify(tokens, null, 2));
    
    console.log(` Successfully wrote ${tokens.length} valid JWTs to valid_tokens.json`);
    process.exit(0);

  } catch (error) {
    console.error(` Seeder Error: ${error.message}`);
    process.exit(1);
  }
};

seedUsers();