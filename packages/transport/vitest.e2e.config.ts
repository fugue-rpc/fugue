import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/e2e.test.ts"],
    globalSetup: ["./e2e-global-setup.ts"],
    // Node.js v22 has native WebSocket — no jsdom needed.
    environment: "node",
    testTimeout: 15_000,
  },
});
