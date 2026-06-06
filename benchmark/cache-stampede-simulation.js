import axios from "axios";

const TARGET_URL = "http://127.0.0.1:5000/api/products/test-mutex";

async function simulateStampede() {
  console.log("Initializing 10 concurrent requests simultaneously...");
  
  // Create an array of 10 simultaneous HTTP requests
  const requests = Array.from({ length: 10 }).map((_, index) => 
    axios.get(TARGET_URL).then(res => ({
      id: index + 1,
      status: res.status,
      msg: res.data.message || "Success"
    })).catch(err => ({
      id: index + 1,
      status: err.response?.status || 500,
      msg: err.message
    }))
  );

  // Execute them all at the exact same moment
  const results = await Promise.all(requests);
  
  console.table(results);
}

simulateStampede();