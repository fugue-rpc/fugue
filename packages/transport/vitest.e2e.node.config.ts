import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/e2e.test.ts"],
    globalSetup: ["./e2e-global-setup.node.ts"],
    environment: "node",
    testTimeout: 15_000,
  },
});
