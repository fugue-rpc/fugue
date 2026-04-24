import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    // Point workspace packages at their TypeScript source so no build step is
    // needed before running the demo.
    alias: {
      "@grpcws/transport": resolve(__dirname, "../../packages/transport/src/index.ts"),
      "@grpcws/react": resolve(__dirname, "../../packages/react/src/index.ts"),
      "@gen": resolve(__dirname, "../../gen/ts"),
    },
  },
  server: {
    port: 5173,
    // Proxy WebSocket traffic to the echo server so the browser never crosses
    // origins (avoids needing WithOrigins("*") in production-like testing).
    proxy: {
      "/wsgrpc": {
        target: "ws://localhost:8080",
        ws: true,
      },
    },
  },
});
