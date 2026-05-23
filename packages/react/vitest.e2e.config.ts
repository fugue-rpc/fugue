import { defineConfig } from "vitest/config";
import path from "path";

export default defineConfig({
  resolve: {
    alias: {
      "@fugue-rpc/transport": path.resolve(__dirname, "../transport/src/index.ts"),
    },
  },
  test: {
    include: ["src/e2e-stress.test.tsx"],
    globalSetup: ["./e2e-global-setup.ts"],
    environment: "jsdom",
    testTimeout: 120_000,
  },
});
