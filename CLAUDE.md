# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

fugue enables all four gRPC RPC kinds (unary, server-streaming, client-streaming, bidirectional) from the browser over a single long-lived WebSocket connection. gRPC-Web and Connect-ES cannot do client-streaming or bidi from browsers because the Fetch API buffers request bodies; this library closes that gap.

Components:
- `fugue-go/` — Go server library, published as `github.com/fugue-rpc/fugue`
- `packages/transport/` — TypeScript browser client (`@fugue-rpc/transport`)
- `packages/react/` — React hooks (`@fugue-rpc/react`)
- `packages/protoc-gen-fugue/` — protoc plugin that generates typed TypeScript clients
- `examples/echo-server/` — reference echo server (all 4 RPC kinds)
- `examples/stress/` — latency/throughput benchmark tool
- `examples/demo/` — browser demo app (Vite + React)
- `docs/wire-format.md` — canonical wire protocol spec

## Commands

### Go
```bash
# All tests (run from repo root — go.work covers workspace)
go test ./fugue-go/... -race

# Single test
go test ./fugue-go/... -run TestName -race

# Run examples
go run ./examples/echo-server        # gRPC-over-WS on :8080/fugue/
go run ./examples/stress -help       # benchmark tool — see -mode, -conns, -streams flags
```

### TypeScript
```bash
pnpm install          # install all workspace deps
pnpm build            # build all packages (transport, react, protoc-gen-fugue)
pnpm test             # unit tests in all packages

# Per-package
pnpm --filter @fugue-rpc/transport test
pnpm --filter @fugue-rpc/transport test:e2e   # integration tests — requires echo server on :8080
pnpm --filter @fugue-rpc/react test

# Demo dev server (requires echo server running first)
pnpm --filter fugue-demo dev               # http://localhost:5173
```

### Code generation
```bash
buf generate   # proto/ → go/fugue/ (Go bindings) + gen/ts/ (TypeScript bindings)
```
Note: `buf generate` writes Go output to `go/fugue/`, **not** `fugue-go/`. After running it, manually copy the generated files (`echo/`, `frame/`) from `go/fugue/` into `fugue-go/` to keep the publish-ready copy in sync.

Note: The `protoc-gen-fugue` plugin generates `*_fugue.ts` files.

## Architecture

### Two Go modules

`go/fugue/` and `fugue-go/` both declare `module github.com/fugue-rpc/fugue`. `fugue-go/` is the canonical, publish-ready copy (has its own git repo and README). `go/fugue/` is the dev working copy and also contains `spike/` (experimental proto bindings not meant for publish).

`go.work` and all example `go.mod` replace directives point at `fugue-go/` — that is the copy all tests run against. If you edit Go source, edit in `fugue-go/`; `go/fugue/` drifts unless manually synced.

`fugue-go/` is also a **separate git repository** (has its own `.git/`). Changes made there need to be committed and pushed independently from this monorepo.

### Wire protocol

Full spec: `docs/wire-format.md`. Short version:

Every frame is a **9-byte header** (1 byte type, 4 bytes stream_id big-endian, 4 bytes payload_length big-endian) followed by a variable payload. Five frame types: `BEGIN` (0x01), `MSG` (0x02), `END` (0x03), `RESET` (0x04), `HEADER` (0x06). Stream IDs are client-assigned, monotonically increasing from 1.

BEGIN / END / HEADER carry protobuf payloads (`BeginPayload`, `EndPayload`, `HeaderPayload` defined in `proto/grpcws/frame/v1/frame.proto`). MSG carries a raw serialized user message. Multiple frames can be coalesced into one WebSocket binary message; receivers must call `DecodeAll` / equivalent.

Understanding the protocol requires reading `docs/wire-format.md`, `fugue-go/frame/frame.go`, and `proto/grpcws/frame/v1/frame.proto` together.

Note: The protobuf package is still `grpcws.frame.v1` — this is part of the wire format and is not renamed.

### Go server internals

The entry point is `fugue-go/server.go`. It upgrades HTTP → WebSocket and creates a `conn.Conn` (`fugue-go/internal/conn/conn.go`) which owns the full lifecycle of one connection:

- **Read loop** (`conn.Serve`): single goroutine, calls `frame.DecodeAll` on each WebSocket message, dispatches frames to per-stream handlers. **The read loop must never block** — this is a correctness invariant, not a performance note. If a stream's recv buffer is full, `Deliver()` returns false, that stream is reset with RESOURCE_EXHAUSTED, and the read loop continues immediately. A blocking `Deliver()` would cause head-of-line blocking across all streams on the connection, defeating the library's core purpose.
- **Writer goroutine**: single goroutine per connection, drains a buffered `writeQueue` channel, coalesces up to 32 frames or 64 KiB into one `ws.Write` call. MSG frames are fire-and-forget enqueues; HEADER and END frames block the caller on a `done` channel until the write completes.
- **Stream** (`fugue-go/internal/stream/stream.go`): implements `grpc.ServerStream`. `SendEnd` is idempotent via `sync.Once` so it can be called from both the read loop (on buffer overflow) and the handler dispatch path safely. `SendMsg` auto-flushes a HEADER frame on its first call.
- **Origin check**: performed at WebSocket upgrade time in `server.go`. Requests with a disallowed `Origin` header are hard-rejected (connection closed). Requests with *no* `Origin` header are allowed — non-browser clients (Go test clients, stress tool) don't send one.

### TypeScript client internals

`packages/transport/src/transport.ts` — `FugueTransport` owns the WebSocket and a `Map<streamId, RawStream>`. `openStream(method)` assigns the next stream ID, sends BEGIN, and returns a `RawStream`.

`packages/transport/src/raw-stream.ts` — `RawStream` implements all four call shapes (unary/server-stream/client-stream/bidi) using an internal async queue with a waiter pattern. Incoming MSG payloads are deserialized by a user-supplied decoder closure. The four shapes (`UnaryCall`, `ServerStream`, `ClientStream`, `BidiStream`) are returned by factory methods on `RawStream`, not separate classes.

Generated clients (`gen/ts/**/_fugue.ts`) wrap `transport.openStream()` with typed serialization using `@bufbuild/protobuf`'s `toBinary` / `fromBinary`. These are generated by `protoc-gen-fugue` from service definitions.

### E2E test wiring

`packages/transport/vitest.e2e.config.ts` uses `e2e-global-setup.ts` which spawns `go run ./examples/echo-server` and waits for `:8080` before running tests. Run with:
```bash
pnpm --filter @fugue-rpc/transport test:e2e
```

### Demo wiring

`examples/demo/vite.config.ts` aliases `@fugue-rpc/transport` and `@fugue-rpc/react` to their TypeScript source (no build step needed), and `@gen` to `gen/ts/`. The dev server proxies `/fugue` WebSocket traffic to `:8080`. Run `buf generate` before starting the demo if `gen/ts/` is missing or stale.

### Codec interface

`fugue-go/codec.go` defines a `Codec` interface (`Marshal`, `Unmarshal`, `Name`). When no custom codec is set, `SendMsg` uses a zero-copy fast path that marshals proto bytes directly into a pooled frame buffer. Custom codecs (e.g. vtprotobuf) can be injected via `WithCodec()`.

The TypeScript equivalent is the decoder closure passed to each call factory: `(bytes: Uint8Array) => T`.

### Performance notes

- `useServerStream` and `useBidiStream` in `@fugue-rpc/react` copy the accumulated messages array on every incoming message (O(n²) total). Fine for typical use; high-frequency streams should use `transport.openStream()` directly.
- The stress tool (`examples/stress/`) includes `-cpuprofile` and `-memprofile` flags and is the right tool for performance regressions.
