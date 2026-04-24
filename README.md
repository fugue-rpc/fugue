# grpcws

gRPC over WebSocket — all four RPC kinds (unary, server-streaming, client-streaming, bidi) in the browser over a single long-lived WebSocket connection.

gRPC-Web and Connect-ES only support unary and server-streaming calls from the browser. grpcws adds client-streaming and bidirectional streaming by multiplexing gRPC streams over one WebSocket using a compact binary framing protocol.

> **Status:** v0.1 pre-release. All four RPC kinds are implemented and tested end-to-end. Not yet published to npm/pkg.go.dev.

---

## When to use grpcws vs Connect-ES

| | grpcws | Connect-ES |
|---|---|---|
| Unary RPC | ✅ | ✅ |
| Server-streaming | ✅ | ✅ |
| Client-streaming from browser | ✅ | ❌ Fetch API buffers full request |
| Bidirectional streaming from browser | ✅ | ❌ Same constraint |
| Protocol overhead at low concurrency | WebSocket framing | Lower (plain HTTP/1.1) |
| Throughput at high concurrency (1k+ streams) | Stays flat | Collapses (HTTP/1.1 connection limits) |
| Infrastructure | Single WebSocket endpoint | HTTP server, any reverse proxy |

**Choose grpcws if:**
- You need client-streaming or bidirectional streaming from a browser. This is grpcws's reason for existing — Connect-ES structurally cannot support these RPC kinds from a browser because the Fetch API buffers the entire request body before the server sees any bytes.
- You expect more than a few hundred concurrent in-flight RPCs from a single connection. At 1,000 concurrent streams, grpcws is 7× faster than Connect-ES (see benchmarks below).
- You are doing server-streaming with many messages per stream. At 100 msgs/stream, grpcws delivers 6.8× the message throughput of Connect-ES.

**Choose Connect-ES if:**
- You only need unary and server-streaming, and concurrency stays below ~200 streams per connection. Connect-ES has lower per-request overhead in this regime and is simpler to deploy (no WebSocket-aware proxy required).
- Your infrastructure requires a plain HTTP reverse proxy (nginx, Cloudflare, etc.) without WebSocket pass-through configured.

---

## Benchmarks

All numbers measured on Windows 11, i7-11700F @ 2.50 GHz, Go 1.26, loopback (127.0.0.1).  
Errors shown are timing artifacts — goroutines that had an in-flight stream when the deadline expired.

### Unary RPC throughput

```
stress -mode grpcws    -conns 10 -streams 10  -duration 30s   (100 concurrent streams)
stress -mode connect-h1 -conns 10 -streams 10  -duration 30s
stress -mode grpcws    -conns 10 -streams 100 -duration 30s   (1000 concurrent streams)
stress -mode connect-h1 -conns 10 -streams 100 -duration 30s
```

| Concurrent streams | grpcws | Connect-ES (H1) | Ratio |
|---|---|---|---|
| 100 (10 conns × 10) | **46,797 RPC/s** · p99 8 ms | 40,402 RPC/s · p99 14 ms | +16% |
| 1,000 (10 conns × 100) | **53,043 RPC/s** · p99 78 ms | 7,443 RPC/s · p99 3,079 ms · 0.6% err | **+7.1×** |

At 1,000 concurrent streams Connect-ES degrades severely — HTTP/1.1 keep-alive serialises requests per connection, so 100 goroutines fighting over 10 TCP sockets creates a queue that pushes p99 latency above 3 seconds. grpcws multiplexes all 1,000 streams over 10 WebSocket connections; p99 stays under 80 ms.

### Server-streaming throughput (100 messages per stream)

```
stress -mode stream-server  -conns 10 -streams 10 -msgs-per-stream 100 -duration 30s
stress -mode connect-stream -conns 10 -streams 10 -msgs-per-stream 100 -duration 30s
```

| | grpcws | Connect-ES (H1) | Ratio |
|---|---|---|---|
| Streams/s | **15,575** | 2,293 | **+6.8×** |
| Messages/s | **1,557,537** | 229,317 | **+6.8×** |
| TTFM p50 | **3.5 ms** | 13.9 ms | **4.0× lower** |
| TTFM p99 | **19 ms** | 152 ms | **8.0× lower** |

The grpcws write queue batches multiple MSG frames into a single WebSocket send (frame coalescing), which halves the number of kernel syscalls at high message rates — this is the primary source of the message-throughput advantage.

### Streaming modes only grpcws supports

```
stress -mode stream-client -conns 10 -streams 10 -msgs-per-stream 100 -duration 30s
stress -mode stream-bidi   -conns 10 -streams 10 -msgs-per-stream 100 -duration 30s
```

| Mode | RPC/s | Messages/s | Connect-ES |
|---|---|---|---|
| Client-streaming (100 msgs → 1 reply) | 2,578 | 257,783 | ❌ impossible from browser |
| Bidirectional streaming (100 msgs ↔ 100 replies) | 743 | 74,270 | ❌ impossible from browser |

### Go micro-benchmarks (single connection, in-process loopback)

```
go test -bench=BenchmarkUnaryEcho -benchmem
```

```
BenchmarkUnaryEcho-16           212 µs/op    121 allocs/op
BenchmarkUnaryEchoParallel-16    40 µs/op    121 allocs/op
```

The allocation count is dominated by proto.Marshal/Unmarshal of user message types. Plugging in a vtprotobuf-backed `Codec` (see [Pluggable Codec](#pluggable-codec) below) eliminates reflection and reduces allocations to the wire-format structs only.

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

The server-side write path uses a single writer goroutine per connection with a buffered channel queue. MSG frames are fire-and-forget (no delivery confirmation); HEADER and END frames block until written. The writer goroutine coalesces frames into batches (up to 64 KiB or 32 frames) and sends each batch in one WebSocket message, reducing syscall overhead by 10–50× at high concurrency.

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
  connect-echo-server/  Connect-ES echo server (unary + server-streaming)
  stress/               Latency + throughput stress tool (all four RPC kinds + Connect-ES comparison)

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
    wsgrpc.WithStreamRecvBuffer(64), // per-stream inbound buffer depth
)
echov1.RegisterEchoServer(srv, &echoImpl{})

mux := http.NewServeMux()
mux.Handle("/wsgrpc/", srv)
http.ListenAndServe(":8080", mux)
```

### Server options

| Option | Default | Description |
|---|---|---|
| `WithOrigins(origins...)` | all allowed | Allowed WebSocket origins; set in production |
| `WithMaxConcurrentStreams(n)` | unlimited | Streams over the limit get RESOURCE_EXHAUSTED |
| `WithStreamRecvBuffer(n)` | 64 | Per-stream inbound buffer depth (slots); full buffer → RESOURCE_EXHAUSTED |
| `WithLogger(l)` | slog.Default() | Logger for connection-level events |
| `WithCodec(c)` | proto | Pluggable message codec (see below) |

### Pluggable Codec

```go
type Codec interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(data []byte, v any) error
    Name() string
}
```

The default codec uses `google.golang.org/protobuf/proto`. To plug in vtprotobuf (zero-reflection marshal/unmarshal):

```go
type vtCodec struct{}

func (vtCodec) Marshal(v any) ([]byte, error) {
    return v.(vtproto.Marshaler).MarshalVT()
}
func (vtCodec) Unmarshal(data []byte, v any) error {
    return v.(vtproto.Unmarshaler).UnmarshalVT(data)
}
func (vtCodec) Name() string { return "vtprotobuf" }

srv := wsgrpc.NewServer(wsgrpc.WithCodec(vtCodec{}))
```

When no custom codec is set, `SendMsg` uses a zero-copy fast path that marshals proto bytes directly into the pooled frame buffer (one fewer allocation and memory copy per outbound message).

### Running tests

```bash
cd go/wsgrpc
go test ./... -race
```

---

## TypeScript client

```bash
pnpm install
```

### Transport

```typescript
import { WsGrpcTransport } from "@grpcws/transport";

const transport = new WsGrpcTransport("wss://app.example.com/wsgrpc/", {
  debug: true, // logs every frame individually via console.debug
});
```

The `debug` option logs each decoded frame separately even when the server coalesces multiple frames into one WebSocket message, so the output always corresponds 1:1 to protocol events.

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

| Milestone | Status |
|-----------|--------|
| Frame codec (Go + TypeScript) | ✅ |
| WebSocket connection manager, stream multiplexer | ✅ |
| gRPC dispatch layer, echo server example | ✅ |
| TypeScript transport implementation | ✅ |
| `protoc-gen-wsgrpc` code generator | ✅ |
| `@grpcws/react` hooks | ✅ |
| End-to-end integration, publish prep | ✅ |
| Write-queue + frame coalescing (eliminate mutex bottleneck) | ✅ |
| Zero-copy frame encoding (proto directly into pooled buffer) | ✅ |
| Pluggable Codec interface | ✅ |
| Publish to npm / pkg.go.dev | ⬜ |

---

## License

See [LICENSE](LICENSE).
