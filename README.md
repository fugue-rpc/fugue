# grpcws

gRPC over WebSocket — all four RPC kinds (unary, server-streaming, client-streaming, bidi) in the browser over a single long-lived WebSocket connection.

gRPC-Web and Connect-ES only support unary and server-streaming calls from the browser. grpcws adds client-streaming and bidirectional streaming by multiplexing gRPC streams over one WebSocket using a compact binary framing protocol.

> **Status:** v0.1 pre-release. All four RPC kinds are implemented and tested end-to-end. Not yet published to npm/pkg.go.dev.

---

## How it works

All gRPC streams share one WebSocket connection. Each stream gets a client-assigned integer ID. Five frame types carry the lifecycle of every RPC:

| Frame   | Hex    | Purpose                                      |
|---------|--------|----------------------------------------------|
| BEGIN   | `0x01` | Open a new stream, carries method + metadata |
| MSG     | `0x02` | One serialised protobuf message              |
| END     | `0x03` | Half-close (client done sending) or EOS      |
| RESET   | `0x04` | Abort a stream immediately                   |
| HEADER  | `0x06` | Server response headers (initial metadata)  |

Full spec: [`docs/wire-format.md`](docs/wire-format.md)

---

## Repository layout

```
go/wsgrpc/              Go server library
  frame/                binary frame codec
  internal/conn/        WebSocket connection manager (stream multiplexer)
  internal/stream/      grpc.ServerStream implementation

packages/
  transport/            TypeScript client transport
  react/                React hooks (useUnary, useServerStream, useBidiStream)
  protoc-gen-wsgrpc/    protoc plugin — generates typed TypeScript clients

examples/
  echo-server/          Echo server (all four RPC kinds)
  stress/               Latency + throughput stress tool

proto/                  .proto definitions (wire format messages)
docs/                   Wire format specification
benchmarks/             Saved benchmark baselines
```

---

## Go server library

```go
import "github.com/grpcws/wsgrpc"
```

Minimum Go version: **1.23**. Depends on [`github.com/coder/websocket`](https://github.com/coder/websocket) and `google.golang.org/grpc`.

```go
srv := wsgrpc.NewServer(
    wsgrpc.WithOrigins("https://app.example.com"),
    wsgrpc.WithMaxConcurrentStreams(1000),
)
echov1.RegisterEchoServer(srv, &echoImpl{})

mux := http.NewServeMux()
mux.Handle("/wsgrpc/", srv)
http.ListenAndServe(":8080", mux)
```

### Running tests

```bash
cd go/wsgrpc
go test ./... -race
```

### Benchmarks (Windows/amd64, i7-11700F, Go 1.26)

**Go micro-benchmarks (single connection, in-process)**

```
BenchmarkUnaryEcho-16           ~82 µs/op    130 allocs/op
BenchmarkUnaryEchoParallel-16   ~19 µs/op    130 allocs/op
BenchmarkBidiEcho-16            ~46 µs/op     40 allocs/op
BenchmarkConnRoundTrip-16       ~94 µs/op    141 allocs/op
```

**Stress tool comparison: grpcws vs Connect-RPC (HTTP/1.1), Windows loopback**

| scenario | grpcws | Connect-H1 |
|---|---|---|
| 1 stream (sequential) | ~8 k RPC/s | ~8 k RPC/s |
| 10 conns × 10 streams | **60 k RPC/s** · p99 8 ms | **60 k RPC/s** · p99 10 ms |
| 50 conns × 20 streams | **76 k RPC/s** · p99 73 ms | 41 k RPC/s · p99 149 ms |

At moderate concurrency the two protocols are equivalent. At high concurrency grpcws gains ~1.9× because WebSocket multiplexing keeps TCP connections at O(conns), while HTTP/1.1 keep-alive requires O(conns × streams) connections — the OS TCP stack starts struggling around 1000 simultaneous connections on Windows.

**Connect server under wrk (WSL→Windows virtual-network hop)**

| connections | RPC/s | p50 | p99 |
|---|---|---|---|
| 1 | 1,200 | 293 µs | 151 ms |
| 100 | 47 k | 1.6 ms | 14 ms |
| 1000 | 45 k | 16 ms | 225 ms |

Numbers are lower than loopback because of the WSL virtual-network adapter latency (~1–3 ms base). See `benchmarks/comparison-results.txt` for full details.

The unary round-trip is ~82 µs on in-process loopback; a grpc-go in-process unary is typically 20–40 µs. The gap is proto.Marshal/Unmarshal allocations — reducing further requires proto message pooling or a custom codec.

---

## TypeScript client

```bash
pnpm install
```

### Transport

```typescript
import { WsGrpcTransport } from "@grpcws/transport";

const transport = new WsGrpcTransport("wss://app.example.com/wsgrpc/", {
  debug: true, // logs every frame via console.debug — useful when debugging CI timeouts
});
```

### React hooks

```tsx
import { WsGrpcProvider, useUnary, useBidiStream } from "@grpcws/react";

function App() {
  return (
    <WsGrpcProvider transport={transport}>
      <Chat />
    </WsGrpcProvider>
  );
}

function Chat() {
  const { state, open, send, halfClose } = useBidiStream(
    () => new EchoClient(transport).echoBidi(),
  );

  return (
    <>
      <button onClick={() => open("hello")}>Connect</button>
      <button onClick={() => send("ping")}>Send</button>
      <ul>{state.status === "open" && state.messages.map((m, i) => <li key={i}>{m}</li>)}</ul>
    </>
  );
}
```

> **Note:** `useServerStream` and `useBidiStream` copy the accumulated messages array on each incoming message (O(n) per message, O(n²) total). This is fine for typical use cases (a few hundred messages per stream). For high-frequency tick streams, consume `transport.openStream()` directly.

### Running tests

```bash
# Unit tests (all packages)
pnpm test

# E2E integration tests (TypeScript transport ↔ real Go echo server)
cd packages/transport
pnpm test:e2e
```

---

## Code generation

Install [buf](https://buf.build) and run:

```bash
buf generate
```

This generates protobuf bindings into `gen/ts/` and `go/wsgrpc/`. The `protoc-gen-wsgrpc` plugin generates typed TypeScript client stubs.

---

## Roadmap

| Week | Milestone | Status |
|------|-----------|--------|
| 1    | Frame codec (Go + TypeScript) | ✅ |
| 2    | WebSocket connection manager, stream multiplexer | ✅ |
| 3    | gRPC dispatch layer, echo server example | ✅ |
| 4    | TypeScript transport implementation | ✅ |
| 5    | `protoc-gen-wsgrpc` code generator | ✅ |
| 6    | `@grpcws/react` hooks | ✅ |
| 7    | End-to-end integration, publish prep | ✅ |

---

## License

See [LICENSE](LICENSE).
