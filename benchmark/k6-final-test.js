import http from 'k6/http';
import { check, sleep } from 'k6';
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';
import { Counter, Rate, Trend } from 'k6/metrics';

const successRate = new Rate('successful_checkouts');
const rateLimitCounter = new Counter('http_429_rate_limited');
const outOfStockCounter = new Counter('http_400_out_of_stock');
const serverErrorCounter = new Counter('http_500_server_error');
const checkoutLatency = new Trend('checkout_duration');

export const options = {
  stages: [
    { duration: '10s', target: 500 },  // 500 concurrent connections
    { duration: '30s', target: 3000 }, // Aggressive Spam
    { duration: '10s', target: 0 },    // Cooldown
  ],
  thresholds: {
    'http_500_server_error': ['count < 50'], 
    'checkout_duration': ['p(95) < 300'],    
  },
};

const JWT_SECRET = 'abac12afsdkjladf'; 

const userPool = [];
for (let i = 0; i < 100; i++) {
  const header = encoding.b64encode(JSON.stringify({ alg: 'HS256', typ: 'JWT' }), 'rawurl');
  const payload = encoding.b64encode(JSON.stringify({ 
    userId: `aggressive_user_${i}`,
    iat: Math.floor(Date.now() / 1000) 
  }), 'rawurl');
  const signature = crypto.hmac('sha256', JWT_SECRET, `${header}.${payload}`, 'base64rawurl');
  userPool.push(`${header}.${payload}.${signature}`);
}

export default function () {
  const token = userPool[Math.floor(Math.random() * userPool.length)];
  
  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`
  };

  const payload = JSON.stringify({
    orderItems: [{ product: '6a25615ba460adc08a331aef', qty: 1, price: 16999 }],
    shippingAddress: { address: '123 Load St', city: 'Test', postalCode: '000', country: 'US' },
    paymentMethod: 'PayPal',
  });

  const checkoutRes = http.post('http://localhost:5000/api/orders', payload, { headers });

  if (checkoutRes.status === 201) {
    checkoutLatency.add(checkoutRes.timings.duration);
    successRate.add(1, true);
  } else {
    successRate.add(0, true);
  }

  if (checkoutRes.status === 429) rateLimitCounter.add(1);
  if (checkoutRes.status === 400) outOfStockCounter.add(1);
  if (checkoutRes.status === 500) serverErrorCounter.add(1);

  check(checkoutRes, {
    'is status 201': (r) => r.status === 201,
    'is status 429 (Rate Limited)': (r) => r.status === 429,
    'is status 400 (Out of Stock)': (r) => r.status === 400,
  });

  sleep(0.5); 
}