import mongoose from "mongoose";
import dotenv from "dotenv";
import Product from "./models/productModel.js";
import Category from "./models/categoryModel.js";
import User from "./models/userModel.js";
import bcrypt from "bcryptjs";

dotenv.config({ path: "../.env" });

// Sample categories
const categories = [
  { name: "Electronics" },
  { name: "Clothing" },
  { name: "Home & Garden" },
  { name: "Sports & Outdoors" },
  { name: "Books & Stationery" },
];

// Sample admin user
const adminUser = {
  username: "admin",
  email: "admin@example.com",
  password: bcrypt.hashSync("123456", 10),
  isAdmin: true,
};

const reviewers = [
  { username: "rahul_sharma", email: "rahul@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "priya_patel", email: "priya@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "amit_kumar", email: "amit@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "sneha_gupta", email: "sneha@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "vikram_singh", email: "vikram@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "anjali_verma", email: "anjali@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "rohan_mehta", email: "rohan@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "pooja_reddy", email: "pooja@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "arjun_nair", email: "arjun@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "kavita_joshi", email: "kavita@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "sanjay_rao", email: "sanjay@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "meera_iyer", email: "meera@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "aditya_chopra", email: "aditya@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "divya_menon", email: "divya@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "karan_malhotra", email: "karan@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "neha_agarwal", email: "neha@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "rajesh_pillai", email: "rajesh@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "sunita_deshmukh", email: "sunita@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "vivek_saxena", email: "vivek@example.com", password: bcrypt.hashSync("123456", 10) },
  { username: "ankita_behera", email: "ankita@example.com", password: bcrypt.hashSync("123456", 10) }
];

// Comprehensive product catalog
const getProducts = (categoryIds) => [
  // ELECTRONICS - Headphones
  {
    name: "Wireless Bluetooth Headphones Pro",
    image: "https://images.unsplash.com/photo-1505740420928-5e560c06d30e?w=400&h=400&fit=crop",
    brand: "SoundMax",
    quantity: 50,
    category: categoryIds[0],
    description: "Premium wireless Bluetooth headphones with active noise cancellation, 40-hour battery life, and crystal clear sound quality. Perfect for music lovers and professionals.",
    rating: 4.8,
    numReviews: 156,
    price: 16999,
    countInStock: 50,
  },
  {
    name: "Bluetooth Headphones Sport Edition",
    image: "https://images.unsplash.com/photo-1484704849700-f032a568e944?w=400&h=400&fit=crop",
    brand: "FitSound",
    quantity: 75,
    category: categoryIds[0],
    description: "Sweat-resistant Bluetooth headphones designed for athletes. Secure fit, 20-hour battery, and powerful bass for workout motivation.",
    rating: 4.5,
    numReviews: 89,
    price: 6499,
    countInStock: 75,
  },
  {
    name: "Kids Bluetooth Headphones",
    image: "https://images.unsplash.com/photo-1583394838336-acd977736f90?w=400&h=400&fit=crop",
    brand: "KidTunes",
    quantity: 100,
    category: categoryIds[0],
    description: "Safe volume-limited Bluetooth headphones for children. Colorful design, durable build, and 15-hour battery life.",
    rating: 4.6,
    numReviews: 234,
    price: 2999,
    countInStock: 100,
  },
  {
    name: "Studio Monitor Bluetooth Headphones",
    image: "https://images.unsplash.com/photo-1545127398-14699f92334b?w=400&h=400&fit=crop",
    brand: "AudioPro",
    quantity: 30,
    category: categoryIds[0],
    description: "Professional studio-quality Bluetooth headphones with flat frequency response. Ideal for music production and mixing.",
    rating: 4.9,
    numReviews: 67,
    price: 28999,
    countInStock: 30,
  },
  {
    name: "Budget Wireless Bluetooth Headphones",
    image: "https://images.unsplash.com/photo-1577174881658-0f30ed549adc?w=400&h=400&fit=crop",
    brand: "ValueSound",
    quantity: 200,
    category: categoryIds[0],
    description: "Affordable Bluetooth headphones with decent sound quality. Great for everyday use, commuting, and casual listening.",
    rating: 4.0,
    numReviews: 456,
    price: 1499,
    countInStock: 200,
  },
  {
    name: "Gaming Bluetooth Headphones RGB",
    image: "https://images.unsplash.com/photo-1599669454699-248893623440?w=400&h=400&fit=crop",
    brand: "GameAudio",
    quantity: 60,
    category: categoryIds[0],
    description: "Low-latency Bluetooth gaming headphones with RGB lighting, surround sound, and detachable microphone.",
    rating: 4.7,
    numReviews: 123,
    price: 11999,
    countInStock: 60,
  },
  
  // ELECTRONICS - Smartwatches
  {
    name: "Smart Watch Pro Max",
    image: "https://images.unsplash.com/photo-1523275335684-37898b6baf30?w=400&h=400&fit=crop",
    brand: "TechWear",
    quantity: 40,
    category: categoryIds[0],
    description: "Advanced smartwatch with ECG, blood oxygen monitoring, GPS, and 7-day battery life. Stay connected and healthy.",
    rating: 4.7,
    numReviews: 312,
    price: 32999,
    countInStock: 40,
  },
  {
    name: "Fitness Smart Watch",
    image: "https://images.unsplash.com/photo-1508685096489-7aacd43bd3b1?w=400&h=400&fit=crop",
    brand: "FitTrack",
    quantity: 80,
    category: categoryIds[0],
    description: "Fitness-focused smartwatch with heart rate monitor, sleep tracking, and 50+ workout modes. Water-resistant to 50m.",
    rating: 4.4,
    numReviews: 189,
    price: 12499,
    countInStock: 80,
  },
  {
    name: "Kids Smart Watch GPS",
    image: "https://images.unsplash.com/photo-1575311373937-040b8e1fd5b6?w=400&h=400&fit=crop",
    brand: "KidSafe",
    quantity: 120,
    category: categoryIds[0],
    description: "GPS smartwatch for kids with real-time location tracking, SOS button, and two-way calling. Keep your children safe.",
    rating: 4.3,
    numReviews: 278,
    price: 4999,
    countInStock: 120,
  },
  
  // ELECTRONICS - Cameras
  {
    name: "4K Ultra HD Camera",
    image: "https://images.unsplash.com/photo-1516035069371-29a1b244cc32?w=400&h=400&fit=crop",
    brand: "PhotoPro",
    quantity: 25,
    category: categoryIds[0],
    description: "Professional 4K mirrorless camera with advanced autofocus, image stabilization, and 45MP sensor. Perfect for photographers.",
    rating: 4.9,
    numReviews: 89,
    price: 109999,
    countInStock: 25,
  },
  {
    name: "Action Camera Waterproof",
    image: "https://images.unsplash.com/photo-1526170375885-4d8ecf77b99f?w=400&h=400&fit=crop",
    brand: "AdventureCam",
    quantity: 90,
    category: categoryIds[0],
    description: "Rugged 4K action camera waterproof to 30m. Wide-angle lens, image stabilization, and WiFi connectivity.",
    rating: 4.5,
    numReviews: 234,
    price: 15999,
    countInStock: 90,
  },
  {
    name: "Instant Print Camera",
    image: "https://images.unsplash.com/photo-1526170375885-4d8ecf77b99f?w=400&h=400&fit=crop",
    brand: "InstaPrint",
    quantity: 150,
    category: categoryIds[0],
    description: "Fun instant camera that prints photos immediately. Great for parties, events, and creating memories.",
    rating: 4.2,
    numReviews: 156,
    price: 5499,
    countInStock: 150,
  },
  
  // ELECTRONICS - Phones & Tablets
  {
    name: "Smartphone Pro 15",
    image: "https://images.unsplash.com/photo-1511707171634-5f897ff02aa9?w=400&h=400&fit=crop",
    brand: "TechPhone",
    quantity: 100,
    category: categoryIds[0],
    description: "Flagship smartphone with 6.7-inch AMOLED display, 108MP camera, 5G connectivity, and all-day battery life.",
    rating: 4.8,
    numReviews: 567,
    price: 84999,
    countInStock: 100,
  },
  {
    name: "Budget Smartphone 5G",
    image: "https://images.unsplash.com/photo-1598327105666-5b89351aff97?w=400&h=400&fit=crop",
    brand: "ValuePhone",
    quantity: 200,
    category: categoryIds[0],
    description: "Affordable 5G smartphone with great camera, large display, and long battery life. Best value for money.",
    rating: 4.3,
    numReviews: 345,
    price: 18999,
    countInStock: 200,
  },
  {
    name: "Tablet Pro 12.9",
    image: "https://images.unsplash.com/photo-1544244015-0df4b3ffc6b0?w=400&h=400&fit=crop",
    brand: "TechTab",
    quantity: 45,
    category: categoryIds[0],
    description: "Powerful tablet with 12.9-inch Liquid Retina display, M2 chip, and all-day battery. Perfect for work and creativity.",
    rating: 4.9,
    numReviews: 234,
    price: 89999,
    countInStock: 45,
  },
  
  // ELECTRONICS - Speakers & Audio
  {
    name: "Portable Bluetooth Speaker",
    image: "https://images.unsplash.com/photo-1608043152269-423dbba4e7e1?w=400&h=400&fit=crop",
    brand: "BoomBox",
    quantity: 150,
    category: categoryIds[0],
    description: "Powerful portable Bluetooth speaker with 360° sound, 24-hour battery, and waterproof design.",
    rating: 4.6,
    numReviews: 456,
    price: 9999,
    countInStock: 150,
  },
  {
    name: "Smart Home Speaker",
    image: "https://images.unsplash.com/photo-1543512214-318c7553f230?w=400&h=400&fit=crop",
    brand: "SmartSound",
    quantity: 200,
    category: categoryIds[0],
    description: "Voice-controlled smart speaker with premium audio. Control your smart home and play music hands-free.",
    rating: 4.4,
    numReviews: 678,
    price: 7999,
    countInStock: 200,
  },
  {
    name: "Wireless Earbuds Pro",
    image: "https://images.unsplash.com/photo-1590658268037-6bf12165a8df?w=400&h=400&fit=crop",
    brand: "SoundMax",
    quantity: 300,
    category: categoryIds[0],
    description: "True wireless earbuds with active noise cancellation, spatial audio, and 30-hour total battery life.",
    rating: 4.7,
    numReviews: 890,
    price: 14999,
    countInStock: 300,
  },
  
  // ELECTRONICS - Laptops & Computers
  {
    name: "Gaming Laptop RTX 4080",
    image: "https://images.unsplash.com/photo-1603302576837-37561b2e2302?w=400&h=400&fit=crop",
    brand: "GameForce",
    quantity: 20,
    category: categoryIds[0],
    description: "High-performance gaming laptop with RTX 4080, 32GB RAM, 1TB SSD, and 240Hz display. Dominate any game.",
    rating: 4.8,
    numReviews: 123,
    price: 199999,
    countInStock: 20,
  },
  {
    name: "Ultrabook Business Laptop",
    image: "https://images.unsplash.com/photo-1496181133206-80ce9b88a853?w=400&h=400&fit=crop",
    brand: "BizPro",
    quantity: 50,
    category: categoryIds[0],
    description: "Thin and light business laptop with 14-inch display, Intel Core i7, 16GB RAM, and 18-hour battery life.",
    rating: 4.6,
    numReviews: 234,
    price: 104999,
    countInStock: 50,
  },
  {
    name: "Student Chromebook",
    image: "https://images.unsplash.com/photo-1588872657578-7efd1f1555ed?w=400&h=400&fit=crop",
    brand: "EduTech",
    quantity: 150,
    category: categoryIds[0],
    description: "Affordable Chromebook perfect for students. Long battery life, lightweight, and runs all essential apps.",
    rating: 4.2,
    numReviews: 567,
    price: 24999,
    countInStock: 150,
  },
  
  // CLOTHING - Jackets & Outerwear
  {
    name: "Classic Leather Jacket",
    image: "https://images.unsplash.com/photo-1551028719-00167b16eac5?w=400&h=400&fit=crop",
    brand: "StyleCraft",
    quantity: 40,
    category: categoryIds[1],
    description: "Timeless genuine leather jacket with premium quality craftsmanship. A wardrobe essential for any style-conscious individual.",
    rating: 4.7,
    numReviews: 189,
    price: 12999,
    countInStock: 40,
  },
  {
    name: "Winter Puffer Jacket",
    image: "https://images.unsplash.com/photo-1544923246-77307dd628b9?w=400&h=400&fit=crop",
    brand: "WarmWear",
    quantity: 100,
    category: categoryIds[1],
    description: "Ultra-warm puffer jacket with down insulation. Windproof and water-resistant for harsh winter conditions.",
    rating: 4.5,
    numReviews: 234,
    price: 7999,
    countInStock: 100,
  },
  {
    name: "Denim Jacket Classic",
    image: "https://images.unsplash.com/photo-1576995853123-5a10305d93c0?w=400&h=400&fit=crop",
    brand: "DenimCo",
    quantity: 80,
    category: categoryIds[1],
    description: "Classic blue denim jacket that never goes out of style. Versatile and durable for everyday wear.",
    rating: 4.4,
    numReviews: 156,
    price: 3499,
    countInStock: 80,
  },
  {
    name: "Rain Jacket Waterproof",
    image: "https://images.unsplash.com/photo-1591047139829-d91aecb6caea?w=400&h=400&fit=crop",
    brand: "OutdoorGear",
    quantity: 120,
    category: categoryIds[1],
    description: "Lightweight waterproof rain jacket with breathable fabric. Packable design for travel and outdoor activities.",
    rating: 4.3,
    numReviews: 278,
    price: 2999,
    countInStock: 120,
  },
  
  // CLOTHING - Footwear
  {
    name: "Running Sneakers Pro",
    image: "https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=400&h=400&fit=crop",
    brand: "SpeedRun",
    quantity: 150,
    category: categoryIds[1],
    description: "High-performance running shoes with responsive cushioning, breathable mesh, and durable outsole. Built for speed.",
    rating: 4.6,
    numReviews: 456,
    price: 8999,
    countInStock: 150,
  },
  {
    name: "Casual Canvas Sneakers",
    image: "https://images.unsplash.com/photo-1525966222134-fcfa99b8ae77?w=400&h=400&fit=crop",
    brand: "StreetStyle",
    quantity: 200,
    category: categoryIds[1],
    description: "Classic canvas sneakers for everyday casual wear. Comfortable, stylish, and available in multiple colors.",
    rating: 4.4,
    numReviews: 567,
    price: 2499,
    countInStock: 200,
  },
  {
    name: "Hiking Boots Waterproof",
    image: "https://images.unsplash.com/photo-1520219306100-ec4afeeefe58?w=400&h=400&fit=crop",
    brand: "TrailMaster",
    quantity: 80,
    category: categoryIds[1],
    description: "Rugged waterproof hiking boots with ankle support and aggressive tread. Conquer any trail with confidence.",
    rating: 4.7,
    numReviews: 234,
    price: 6999,
    countInStock: 80,
  },
  {
    name: "Formal Leather Shoes",
    image: "https://images.unsplash.com/photo-1449505278894-297fdb3edbc1?w=400&h=400&fit=crop",
    brand: "GentlemanStyle",
    quantity: 60,
    category: categoryIds[1],
    description: "Elegant leather dress shoes handcrafted with premium materials. Perfect for business and formal occasions.",
    rating: 4.5,
    numReviews: 123,
    price: 5999,
    countInStock: 60,
  },
  {
    name: "Slip-On Comfort Shoes",
    image: "https://images.unsplash.com/photo-1560343090-f0409e92791a?w=400&h=400&fit=crop",
    brand: "ComfortStep",
    quantity: 180,
    category: categoryIds[1],
    description: "Ultra-comfortable slip-on shoes with memory foam insole. Easy to wear and perfect for all-day comfort.",
    rating: 4.3,
    numReviews: 345,
    price: 1999,
    countInStock: 180,
  },
  
  // CLOTHING - Shirts & Tops
  {
    name: "Cotton Polo Shirt",
    image: "https://images.unsplash.com/photo-1586363104862-3a5e2ab60d99?w=400&h=400&fit=crop",
    brand: "ClassicWear",
    quantity: 200,
    category: categoryIds[1],
    description: "Premium cotton polo shirt with comfortable fit. Perfect for casual and smart-casual occasions.",
    rating: 4.4,
    numReviews: 345,
    price: 1299,
    countInStock: 200,
  },
  {
    name: "Formal Dress Shirt",
    image: "https://images.unsplash.com/photo-1596755094514-f87e34085b2c?w=400&h=400&fit=crop",
    brand: "BusinessPro",
    quantity: 150,
    category: categoryIds[1],
    description: "Crisp formal dress shirt with wrinkle-resistant fabric. Professional look for office and meetings.",
    rating: 4.5,
    numReviews: 234,
    price: 1799,
    countInStock: 150,
  },
  {
    name: "Graphic T-Shirt Pack",
    image: "https://images.unsplash.com/photo-1521572163474-6864f9cf17ab?w=400&h=400&fit=crop",
    brand: "StreetStyle",
    quantity: 300,
    category: categoryIds[1],
    description: "Pack of 3 graphic t-shirts with unique designs. Soft cotton fabric for everyday comfort.",
    rating: 4.2,
    numReviews: 567,
    price: 999,
    countInStock: 300,
  },
  
  // HOME & GARDEN
  {
    name: "Modern Table Lamp LED",
    image: "https://images.unsplash.com/photo-1507473885765-e6ed057f782c?w=400&h=400&fit=crop",
    brand: "HomeGlow",
    quantity: 100,
    category: categoryIds[2],
    description: "Elegant LED table lamp with adjustable brightness and color temperature. Modern design for any room.",
    rating: 4.5,
    numReviews: 189,
    price: 2499,
    countInStock: 100,
  },
  {
    name: "Smart LED Strip Lights",
    image: "https://images.unsplash.com/photo-1558618666-fcd25c85cd64?w=400&h=400&fit=crop",
    brand: "SmartHome",
    quantity: 200,
    category: categoryIds[2],
    description: "WiFi-controlled RGB LED strip lights with music sync. Transform any space with millions of colors.",
    rating: 4.4,
    numReviews: 456,
    price: 999,
    countInStock: 200,
  },
  {
    name: "Indoor Plant Pot Set",
    image: "https://images.unsplash.com/photo-1485955900006-10f4d324d411?w=400&h=400&fit=crop",
    brand: "GreenHome",
    quantity: 150,
    category: categoryIds[2],
    description: "Set of 3 modern ceramic plant pots with drainage holes. Perfect for succulents and small plants.",
    rating: 4.6,
    numReviews: 234,
    price: 1499,
    countInStock: 150,
  },
  {
    name: "Kitchen Knife Set Professional",
    image: "https://images.unsplash.com/photo-1593618998160-e34014e67546?w=400&h=400&fit=crop",
    brand: "ChefPro",
    quantity: 60,
    category: categoryIds[2],
    description: "Professional 8-piece kitchen knife set with block. High-carbon stainless steel for precision cutting.",
    rating: 4.8,
    numReviews: 345,
    price: 8999,
    countInStock: 60,
  },
  {
    name: "Robot Vacuum Cleaner",
    image: "https://images.unsplash.com/photo-1558618666-fcd25c85cd64?w=400&h=400&fit=crop",
    brand: "CleanBot",
    quantity: 40,
    category: categoryIds[2],
    description: "Smart robot vacuum with mapping technology, auto-charging, and app control. Keep your floors spotless.",
    rating: 4.5,
    numReviews: 567,
    price: 24999,
    countInStock: 40,
  },
  {
    name: "Air Purifier HEPA",
    image: "https://images.unsplash.com/photo-1585771724684-38269d6639fd?w=400&h=400&fit=crop",
    brand: "PureAir",
    quantity: 80,
    category: categoryIds[2],
    description: "HEPA air purifier that removes 99.97% of allergens, dust, and pollutants. Quiet operation for bedrooms.",
    rating: 4.7,
    numReviews: 234,
    price: 12999,
    countInStock: 80,
  },
  {
    name: "Coffee Maker Automatic",
    image: "https://images.unsplash.com/photo-1517668808822-9ebb02f2a0e6?w=400&h=400&fit=crop",
    brand: "BrewMaster",
    quantity: 100,
    category: categoryIds[2],
    description: "Programmable coffee maker with built-in grinder. Wake up to freshly ground coffee every morning.",
    rating: 4.6,
    numReviews: 456,
    price: 7999,
    countInStock: 100,
  },
  {
    name: "Bedding Set King Size",
    image: "https://images.unsplash.com/photo-1522771739844-6a9f6d5f14af?w=400&h=400&fit=crop",
    brand: "DreamSleep",
    quantity: 50,
    category: categoryIds[2],
    description: "Luxury king-size bedding set with 1000-thread count sheets. Ultra-soft Egyptian cotton for the best sleep.",
    rating: 4.8,
    numReviews: 189,
    price: 6999,
    countInStock: 50,
  },
  
  // SPORTS & OUTDOORS
  {
    name: "Yoga Mat Premium",
    image: "https://images.unsplash.com/photo-1601925260368-ae2f83cf8b7f?w=400&h=400&fit=crop",
    brand: "ZenFit",
    quantity: 200,
    category: categoryIds[3],
    description: "Extra-thick premium yoga mat with non-slip surface. Eco-friendly material for comfortable practice.",
    rating: 4.6,
    numReviews: 345,
    price: 1999,
    countInStock: 200,
  },
  {
    name: "Dumbbell Set Adjustable",
    image: "https://images.unsplash.com/photo-1534438327276-14e5300c3a48?w=400&h=400&fit=crop",
    brand: "PowerLift",
    quantity: 60,
    category: categoryIds[3],
    description: "Adjustable dumbbell set from 5-50 lbs. Space-saving design replaces 15 sets of weights.",
    rating: 4.8,
    numReviews: 234,
    price: 18999,
    countInStock: 60,
  },
  {
    name: "Camping Tent 4-Person",
    image: "https://images.unsplash.com/photo-1504280390367-361c6d9f38f4?w=400&h=400&fit=crop",
    brand: "WildCamp",
    quantity: 40,
    category: categoryIds[3],
    description: "Spacious 4-person camping tent with waterproof fly and easy setup. Perfect for family adventures.",
    rating: 4.5,
    numReviews: 189,
    price: 8999,
    countInStock: 40,
  },
  {
    name: "Mountain Bike 27.5",
    image: "https://images.unsplash.com/photo-1532298229144-0ec0c57515c7?w=400&h=400&fit=crop",
    brand: "TrailRider",
    quantity: 25,
    category: categoryIds[3],
    description: "Full-suspension mountain bike with 27.5-inch wheels. Aluminum frame and hydraulic disc brakes.",
    rating: 4.7,
    numReviews: 89,
    price: 45999,
    countInStock: 25,
  },
  {
    name: "Tennis Racket Pro",
    image: "https://images.unsplash.com/photo-1617083934555-6bfa0ef4b52b?w=400&h=400&fit=crop",
    brand: "AcePlay",
    quantity: 80,
    category: categoryIds[3],
    description: "Professional tennis racket with graphite frame and optimal string tension. Improve your game today.",
    rating: 4.4,
    numReviews: 123,
    price: 7499,
    countInStock: 80,
  },
  {
    name: "Running Water Bottle",
    image: "https://images.unsplash.com/photo-1602143407151-7111542de6e8?w=400&h=400&fit=crop",
    brand: "HydroRun",
    quantity: 300,
    category: categoryIds[3],
    description: "Insulated stainless steel water bottle that keeps drinks cold for 24 hours. Leak-proof design.",
    rating: 4.5,
    numReviews: 567,
    price: 899,
    countInStock: 300,
  },
  {
    name: "Resistance Bands Set",
    image: "https://images.unsplash.com/photo-1598289431512-b97b0917affc?w=400&h=400&fit=crop",
    brand: "FlexFit",
    quantity: 250,
    category: categoryIds[3],
    description: "Set of 5 resistance bands with different strengths. Perfect for home workouts and physical therapy.",
    rating: 4.3,
    numReviews: 456,
    price: 699,
    countInStock: 250,
  },
  
  // BOOKS & STATIONERY
  {
    name: "Notebook Premium Leather",
    image: "https://images.unsplash.com/photo-1531346878377-a5be20888e57?w=400&h=400&fit=crop",
    brand: "WriteWell",
    quantity: 150,
    category: categoryIds[4],
    description: "Premium leather-bound notebook with 200 pages of thick paper. Perfect for journaling and note-taking.",
    rating: 4.7,
    numReviews: 234,
    price: 1299,
    countInStock: 150,
  },
  {
    name: "Desk Organizer Set",
    image: "https://images.unsplash.com/photo-1507925921958-8a62f3d1a50d?w=400&h=400&fit=crop",
    brand: "OfficePro",
    quantity: 100,
    category: categoryIds[4],
    description: "Elegant desk organizer set with pen holder, letter tray, and accessory compartments. Keep your workspace tidy.",
    rating: 4.4,
    numReviews: 189,
    price: 1999,
    countInStock: 100,
  },
  {
    name: "Fountain Pen Luxury",
    image: "https://images.unsplash.com/photo-1583485088034-697b5bc54ccd?w=400&h=400&fit=crop",
    brand: "InkMaster",
    quantity: 50,
    category: categoryIds[4],
    description: "Luxury fountain pen with gold nib and elegant design. Writes smoothly and makes a perfect gift.",
    rating: 4.8,
    numReviews: 89,
    price: 4999,
    countInStock: 50,
  },
  {
    name: "Planner 2026 Weekly",
    image: "https://images.unsplash.com/photo-1506784365847-bbad939e9335?w=400&h=400&fit=crop",
    brand: "PlanAhead",
    quantity: 200,
    category: categoryIds[4],
    description: "2026 weekly planner with goal-setting pages, habit trackers, and inspirational quotes. Plan your success.",
    rating: 4.5,
    numReviews: 345,
    price: 599,
    countInStock: 200,
  },
  {
    name: "Art Supply Kit Complete",
    image: "https://images.unsplash.com/photo-1513519245088-0e12902e35ca?w=400&h=400&fit=crop",
    brand: "ArtMaster",
    quantity: 80,
    category: categoryIds[4],
    description: "Complete art supply kit with pencils, paints, brushes, and sketchpad. Everything an artist needs.",
    rating: 4.6,
    numReviews: 156,
    price: 2499,
    countInStock: 80,
  },
];



// Review templates for different ratings
const reviewTemplates = {
  5: [
    { title: "Excellent product!", comment: "Absolutely love it! The quality exceeded my expectations. Highly recommended for everyone." },
    { title: "Best purchase ever", comment: "This is exactly what I was looking for. Fast delivery and amazing quality. Worth every rupee!" },
    { title: "Outstanding quality", comment: "Superb product! The build quality is top-notch. Very happy with my purchase." },
    { title: "Perfect!", comment: "Couldn't be happier. Works exactly as described. Great value for money." },
    { title: "Highly recommended", comment: "After using for 2 weeks, I can say this is one of the best purchases I've made. Totally worth it!" },
  ],
  4: [
    { title: "Very good product", comment: "Really happy with this purchase. Minor issues but overall great value for the price." },
    { title: "Good quality", comment: "Product is good as expected. Delivery was quick. Would recommend to others." },
    { title: "Satisfied customer", comment: "Nice product, works well. Could be slightly better but no major complaints." },
    { title: "Worth buying", comment: "Good product for the price. Does what it's supposed to do. Happy with the purchase." },
    { title: "Recommended", comment: "Quality is good, packaging was excellent. Minor improvement needed but overall satisfied." },
  ],
  3: [
    { title: "Average product", comment: "It's okay, nothing special. Does the job but expected a bit more for this price." },
    { title: "Decent purchase", comment: "Product is average. Not bad but not great either. You get what you pay for." },
    { title: "Could be better", comment: "Satisfactory product. Has some room for improvement. Works fine for basic needs." },
  ],
  2: [
    { title: "Not impressed", comment: "Expected better quality. Product is just okay. Might look for alternatives next time." },
    { title: "Below expectations", comment: "The product is not as good as shown in pictures. Somewhat disappointed." },
  ],
  1: [
    { title: "Not recommended", comment: "Poor quality product. Does not match the description. Very disappointed with this purchase." },
  ]
};

// Generate fake reviews for a product
const generateFakeReviews = (numReviews, avgRating, createdUsers) => {
  const reviews = [];
  // Max out reviews based on our available user pool size
  const reviewCount = Math.min(numReviews, 8, createdUsers.length); 
  
  // Shuffle array to ensure randomized review distribution across products
  const shuffledUsers = [...createdUsers].sort(() => 0.5 - Math.random());

  for (let i = 0; i < reviewCount; i++) {
    let rating;
    const rand = Math.random();
    if (rand < 0.5) rating = Math.round(avgRating);
    else if (rand < 0.75) rating = Math.min(5, Math.round(avgRating) + 1);
    else if (rand < 0.9) rating = Math.max(1, Math.round(avgRating) - 1);
    else rating = Math.random() > 0.5 ? 5 : Math.max(1, Math.round(avgRating) - 2);
    
    rating = Math.max(1, Math.min(5, rating));
    
    const templates = reviewTemplates[rating];
    const template = templates[Math.floor(Math.random() * templates.length)];
    
    // Select the target user document
    const targetUser = shuffledUsers[i];
    
    const daysAgo = Math.floor(Math.random() * 180);
    const reviewDate = new Date();
    reviewDate.setDate(reviewDate.getDate() - daysAgo);
    
    reviews.push({
      user: targetUser._id,               // CRITICAL: The exact Mongo ID for Python to track
      name: targetUser.username,         // Plain text string for the UI display
      rating: rating,
      title: template.title,
      comment: template.comment,
      isVerifiedPurchase: Math.random() > 0.2, 
      helpfulVotes: Math.floor(Math.random() * 50),
      createdAt: reviewDate,
      updatedAt: reviewDate,
    });
  }
  
  return reviews;
};

const seedDatabase = async () => {
  try {
    await mongoose.connect("mongodb+srv://tushankr0020:Mongo123@cluster0.oehhe.mongodb.net/ecommerce?appName=Cluster0");

    console.log("MongoDB Connected");

    // Clear existing data
    await Product.deleteMany({});
    await Category.deleteMany({});
    await User.deleteMany({});
    console.log("Cleared existing collections");

    // Create admin user
    await User.create(adminUser);
    console.log("Admin user created: admin@example.com / 123456");

    // CRITICAL UPDATE: Seed regular buyer accounts first to yield valid IDs
    const createdUsers = await User.insertMany(reviewers);
    console.log(`${createdUsers.length} dummy user accounts generated dynamically`);

    // Create categories
    const createdCategories = await Category.insertMany(categories);
    const categoryIds = createdCategories.map((cat) => cat._id);
    console.log(`${createdCategories.length} categories created`);

    // Create products with relational reviews mapping users
    const products = getProducts(categoryIds);
    const productsWithReviews = products.map(product => ({
      ...product,
      reviews: generateFakeReviews(product.numReviews, product.rating, createdUsers)
    }));
    
    await Product.insertMany(productsWithReviews);
    console.log(`${products.length} products created with valid relational reviews`);

    console.log("\n✅ Database seeded successfully at Amazon ML Standard!");
    
    // Log a few dummy credentials so you can test logging in as a reviewer later
    console.log("\nSample User to login with on the frontend:");
    console.log(`Email: ${reviewers[0].email}`);
    console.log("Password: 123456");

    process.exit(0);
  } catch (error) {
    console.error("Error seeding database:", error);
    process.exit(1);
  }
};

seedDatabase();