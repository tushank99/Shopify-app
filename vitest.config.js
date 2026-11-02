import { defineConfig } from "vitest/config";

// Vitest instead of Jest: the repo is ESM ("type": "module"); Jest needs --experimental-vm-modules
// and does not hoist jest.mock for ESM. The API (describe/it/expect/beforeAll) is the same.
export default defineConfig({
  test: {
    include: ["backend/__tests__/**/*.test.js"],
    environment: "node",
    testTimeout: 30_000,
    hookTimeout: 60_000,
    fileParallelism: false,
    // The outbox write is gated on KAFKA_BROKERS (see backend/outbox/outboxConfig.js).
    // Tests exercise the enabled path, so they must not depend on the developer's shell/.env.
    env: { KAFKA_BROKERS: "localhost:9092", OUTBOX_ENABLED: "true" },
  },
});