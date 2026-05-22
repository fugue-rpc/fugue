# fugue

gRPC over WebSocket — all four RPC kinds (unary, server-streaming, client-streaming, bidi) in the browser over a single long-lived WebSocket connection.

gRPC-Web and Connect-ES only support unary and server-streaming calls from the browser. fugue adds client-streaming and bidirectional streaming by multiplexing gRPC streams over one WebSocket using a compact binary framing protocol.

> **Status:** v0.1 pre-release. All four RPC kinds are implemented and tested end-to-end. Not yet published to npm/pkg.go.dev.

---

## When to use fugue vs Connect-ES

| | fugue | Connect-ES |
|---|---|---|
| Unary RPC | ✅ | ✅ |
| Server-streaming | ✅ | ✅ |
| Client-streaming from browser | ✅ | ❌ Fetch API buffers full request |
| Bidirectional streaming from browser | ✅ | ❌ Same constraint |
| Protocol overhead at low concurrency | WebSocket framing | Lower (plain HTTP/1.1) |
| Throughput at high concurrency (1k+ streams) | Stays flat | Collapses (HTTP/1.1 connection limits) |
| Infrastructure | Single WebSocket endpoint | HTTP server, any reverse proxy |

**Choose fugue if:**
- You need client-streaming or bidirectional streaming from a browser. This is fugue's reason for existing — Connect-ES structurally cannot support these RPC kinds from a browser because the Fetch API buffers the entire request body before the server sees any bytes.
- You expect more than a few hundred concurrent in-flight RPCs from a single connection. At 1,000 concurrent streams, fugue is 14× faster than Connect-ES (see benchmarks below).
- You are doing server-streaming with many messages per stream. At 100 msgs/stream, fugue delivers 24× the message throughput of Connect-ES.

**Choose Connect-ES if:**
- You only need unary and server-streaming, and concurrency stays below ~50 streams per connection. Connect-ES has lower per-request overhead in this regime and is simpler to deploy (no WebSocket-aware proxy required).
- Your infrastructure requires a plain HTTP reverse proxy (nginx, Cloudflare, etc.) without WebSocket pass-through configured.

---

## Benchmarks

All numbers measured on Windows 11, i7-11700F @ 2.50 GHz, Go 1.24, loopback (127.0.0.1).  
Errors shown are timing artifacts — goroutines that had an in-flight stream when the deadline expired.

### Unary RPC throughput

```
stress -mode fugue      -conns 10 -streams 10  -duration 30s   # 100 concurrent streams
stress -mode connect-h1 -conns 10 -streams 10  -duration 30s
stress -mode fugue      -conns 10 -streams 100 -duration 30s   # 1,000 concurrent streams
stress -mode connect-h1 -conns 10 -streams 100 -duration 30s
```

| Concurrent streams | fugue | Connect-ES (H1) | Ratio |
|---|---|---|---|
| 100 (10 conns × 10) | **23,061 RPC/s** · p99 15 ms | 2,599 RPC/s · p99 127 ms · 1.0% err | **+8.9×** |
| 1,000 (10 conns × 100) | **30,657 RPC/s** · p99 94 ms | 2,194 RPC/s · p99 3,692 ms · 4.3% err | **+14×** |

**Why fugue scales and Connect-ES doesn't:**  
HTTP/1.1 keep-alive reuses connections, but only serially — each in-flight request holds its socket exclusively. With 1,000 concurrent goroutines and 10 connections, each connection queues 100 requests one at a time; p99 blows out to 3.7 seconds. fugue multiplexes all 1,000 streams over 10 WebSocket connections with no per-stream socket contention, keeping p99 under 100 ms.

### Server-streaming throughput (100 messages per stream)

```
stress -mode stream-server  -conns 10 -streams 10 -msgs-per-stream 100 -duration 30s
stress -mode connect-stream -conns 10 -streams 10 -msgs-per-stream 100 -duration 30s
```

| | fugue | Connect-ES (H1) | Ratio |
|---|---|---|---|
| Streams/s | **6,996** | 292 | **+24×** |
| Messages/s | **699,603** | 29,150 | **+24×** |
| TTFM p50 | **8 ms** | 76 ms | **9.5× lower** |
| TTFM p99 | **29 ms** | 1,942 ms | **67× lower** |

**Why the gap is larger for streaming:**  
Each Connect-ES streaming response requires a dedicated HTTP connection. With 100 concurrent streams and 10 connections, streams queue serialised connection access for the full duration of 100 messages — a head-of-line blocking effect that compounds across messages. fugue streams share connections at the frame level with no per-stream blocking; the write goroutine coalesces up to 32 MSG frames per WebSocket send, cutting syscall overhead by 10–50× at high message rates.

### Streaming modes only fugue supports

```
stress -mode stream-client -conns 10 -streams 10 -msgs-per-stream 100 -duration 30s
stress -mode stream-bidi   -conns 10 -streams 10 -msgs-per-stream 100 -duration 30s
```

| Mode | RPC/s | Messages/s | Connect-ES |
|---|---|---|---|
| Client-streaming (100 msgs → 1 reply) | 1,347 | 134,710 | impossible from browser |
| Bidirectional streaming (100 msgs ↔ 100 replies) | 923 | 92,317 | impossible from browser |

Connect-ES uses the Fetch API for HTTP requests. The Fetch API requires the full request body to be buffered before any bytes are sent to the server, making it structurally impossible to stream data to the server incrementally. fugue avoids this by owning the WebSocket connection entirely — MSG frames are sent as the application calls `Send()`, with no buffering.

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

The server-side write path uses a single writer goroutine per connection with a buffered channel queue. All frame types (MSG, HEADER, END) are fire-and-forget enqueues — no delivery confirmation, no blocking. FIFO queue ordering preserves the protocol invariant that HEADER always arrives before MSG, and MSG before END. The writer goroutine coalesces frames into batches (up to 64 KiB or 32 frames) and sends each batch in one WebSocket message, reducing syscall overhead at high concurrency.

The read loop never blocks: if a stream's inbound buffer is full, that stream is reset with RESOURCE_EXHAUSTED and the read loop continues immediately. Head-of-line blocking is impossible.

Full spec: [`docs/wire-format.md`](docs/wire-format.md)

---

## Repository layout

```
fugue-go/              Go server library (published as github.com/fugue-rpc/fugue-go)
  frame/                binary frame codec
  internal/conn/        WebSocket connection manager (stream multiplexer)
  internal/stream/      grpc.ServerStream implementation

packages/
  transport/            TypeScript client transport (@fugue-rpc/transport)
  react/                React hooks (@fugue-rpc/react)
  node-server/          Node.js server library (@fugue-rpc/node-server)
  protoc-gen-fugue/     protoc plugin — generates typed TypeScript clients

examples/
  echo-server/          Go echo server (all four RPC kinds, :8080/fugue/)
  connect-echo-server/  Connect-ES echo server (unary + server-streaming, :8090)
  node-echo-server/     Node.js echo server (all four RPC kinds)
  stress/               Latency + throughput stress tool (all modes + Connect-ES comparison)
  demo/                 Browser demo app (Vite + React)

proto/                  .proto definitions (wire format messages)
docs/                   Wire format specification
```

---

## Go server library

```go
import "github.com/fugue-rpc/fugue-go"
```

Minimum Go version: **1.23**. Depends on [`github.com/coder/websocket`](https://github.com/coder/websocket) and `google.golang.org/grpc`.

```go
srv := fugue.NewServer(
    fugue.WithOrigins("https://app.example.com"),
    fugue.WithMaxConcurrentStreams(1000),
    fugue.WithStreamRecvBuffer(256), // per-stream inbound buffer depth
)
echov1.RegisterEchoServer(srv, &echoImpl{})

mux := http.NewServeMux()
mux.Handle("/fugue/", srv)
http.ListenAndServe(":8080", mux)
```

### Server options

| Option | Default | Description |
|---|---|---|
| `WithOrigins(origins...)` | all allowed | Allowed WebSocket origins; set in production |
| `WithMaxConcurrentStreams(n)` | unlimited | Streams over the limit get RESOURCE_EXHAUSTED |
| `WithStreamRecvBuffer(n)` | 256 | Per-stream inbound buffer depth (slots); full buffer → RESOURCE_EXHAUSTED |
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

srv := fugue.NewServer(fugue.WithCodec(vtCodec{}))
```

When no custom codec is set, `SendMsg` uses a zero-copy fast path that marshals proto bytes directly into the pooled frame buffer (one fewer allocation and memory copy per outbound message).

### Running tests

```bash
go test ./fugue-go/... -race
```

---

## TypeScript client

```bash
pnpm install
```

### Transport

```typescript
import { FugueTransport } from "@fugue-rpc/transport";

const transport = new FugueTransport("wss://app.example.com/fugue/", {
  debug: true, // logs every frame individually via console.debug
});
```

The `debug` option logs each decoded frame separately even when the server coalesces multiple frames into one WebSocket message, so the output always corresponds 1:1 to protocol events.

### React hooks

```tsx
import { FugueProvider, useUnary, useBidiStream } from "@fugue-rpc/react";

function App() {
  return (
    <FugueProvider transport={transport}>
      <Chat />
    </FugueProvider>
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

# E2E integration tests (TypeScript transport ↔ real Node.js echo server)
pnpm --filter @fugue-rpc/transport test:e2e
```

---

## Code generation

Install [buf](https://buf.build) and run:

```bash
buf generate
```

This generates protobuf bindings into `gen/ts/` and `fugue-go/`. The `protoc-gen-fugue` plugin generates typed TypeScript client stubs (`*_fugue.ts`).

---

## Roadmap

| Milestone | Status |
|-----------|--------|
| Frame codec (Go + TypeScript) | ✅ |
| WebSocket connection manager, stream multiplexer | ✅ |
| gRPC dispatch layer, echo server example | ✅ |
| TypeScript transport implementation | ✅ |
| `protoc-gen-fugue` code generator | ✅ |
| `@fugue-rpc/react` hooks | ✅ |
| End-to-end integration, publish prep | ✅ |
| Write-queue + frame coalescing (eliminate mutex bottleneck) | ✅ |
| Zero-copy frame encoding (proto directly into pooled buffer) | ✅ |
| Pluggable Codec interface | ✅ |
| Fire-and-forget HEADER/END frames (enable cross-stream coalescing) | ✅ |
| Publish to npm / pkg.go.dev | ⬜ |

---

## License

See [LICENSE](LICENSE).
