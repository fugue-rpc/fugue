import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    // e2e tests spin up real servers and need a longer timeout; run them via test:e2e.
    exclude: ["src/e2e.test.ts"],
  },
});
