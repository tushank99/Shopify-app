/**
 * Exactly ORDERS checkouts, spread over USERS virtual users (one JWT each, so the per-user rate limit of
 * 50 req / 10 s is never the limiting factor at the default 100 orders / 20 users).
 *
 *   k6 run -e ORDERS=100 -e USERS=20 -e SLEEP_S=0.2 benchmark/k6-drill-orders.js
 *
 * Needs benchmark/drill_data.json (node benchmark/drill-seed.js).
 * Writes RESULT_FILE (default benchmark/drill-result.json) with the counts the drill runner reconciles against.
 */
import http from "k6/http";
import { check, sleep } from "k6";
import { Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:5000";
const ORDERS = parseInt(__ENV.ORDERS || "100", 10);
const SLEEP_S = parseFloat(__ENV.SLEEP_S || "0.2");
const RESULT_FILE = __ENV.RESULT_FILE || "benchmark/drill-result.json";

const data = JSON.parse(open("./drill_data.json"));
const USERS = Math.min(parseInt(__ENV.USERS || "20", 10), data.users.length);

const ordersCreated = new Counter("orders_created");
const ordersRejected = new Counter("orders_rejected");

export const options = {
  scenarios: {
    drill: {
      // shared-iterations => exactly ORDERS iterations in total, however the VUs share them.
      executor: "shared-iterations",
      vus: USERS,
      iterations: ORDERS,
      maxDuration: "5m",
    },
  },
  thresholds: {
    orders_created: [`count==${ORDERS}`],
    orders_rejected: ["count==0"],
  },
};

export default function () {
  const user = data.users[(__VU - 1) % USERS];

  const res = http.post(
    `${BASE_URL}/api/orders`,
    JSON.stringify({
      orderItems: [{ _id: data.productId, qty: 1 }],
      shippingAddress: { address: "123 Drill St", city: "Test", postalCode: "000", country: "US" },
      paymentMethod: "PayPal",
    }),
    {
      headers: {
        "Content-Type": "application/json",
        Cookie: `jwt=${user.token}`, // authMiddleware reads the JWT from the `jwt` cookie
      },
      tags: { name: "POST /api/orders" },
    }
  );

  const created = check(res, { "order created (201)": (r) => r.status === 201 });
  if (created) {
    ordersCreated.add(1);
  } else {
    ordersRejected.add(1, { status: String(res.status) });
  }

  sleep(SLEEP_S);
}

export function handleSummary(summary) {
  const count = (name) => {
    const m = summary.metrics[name];
    if (!m) return 0;
    return (m.values || m).count || 0;
  };
  const p95 = (() => {
    const m = summary.metrics.http_req_duration;
    return m ? (m.values || m)["p(95)"] || 0 : 0;
  })();

  const result = {
    orders_created: count("orders_created"),
    orders_rejected: count("orders_rejected"),
    http_reqs: count("http_reqs"),
    http_req_duration_p95_ms: Math.round(p95),
  };

  return {
    [RESULT_FILE]: JSON.stringify(result, null, 2),
    stdout: `\nk6 drill: created=${result.orders_created} rejected=${result.orders_rejected} p95=${result.http_req_duration_p95_ms}ms\n`,
  };
}