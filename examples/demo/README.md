# grpcws demo

React app showing all four RPC kinds (unary, server-streaming, client-streaming, bidi) running live against the echo server.

## Prerequisites

The demo imports generated TypeScript bindings from `gen/ts/`. If that directory is missing or stale, regenerate it first:

```bash
# from repo root
buf generate
```

## Running

Start the echo server in one terminal:

```bash
cd examples/echo-server
go run .
```

Start the dev server in another:

```bash
# from repo root
pnpm --filter grpcws-demo dev
```

Then open http://localhost:5173. The Vite proxy forwards `/wsgrpc` WebSocket traffic to `localhost:8080` so no CORS configuration is needed.
