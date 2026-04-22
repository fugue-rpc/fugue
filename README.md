# grpcws

gRPC over WebSocket — all four RPC kinds (unary, server-streaming, client-streaming, bidi) in the browser over a single long-lived WebSocket connection.

gRPC-Web and Connect-ES only support unary and server-streaming calls from the browser. grpcws adds client-streaming and bidirectional streaming by multiplexing gRPC streams over one WebSocket using a compact binary framing protocol.

> **Status:** early development — Week 2 of 8 complete. Not ready for production use.

---

## How it works

All gRPC streams share one WebSocket connection. Each stream gets a client-assigned integer ID. Six frame types carry the lifecycle of every RPC:

| Frame   | Hex    | Purpose                                      |
|---------|--------|----------------------------------------------|
| BEGIN   | `0x01` | Open a new stream, carries method + metadata |
| MSG     | `0x02` | One serialised protobuf message              |
| END     | `0x03` | Half-close (client done sending) or EOS      |
| RESET   | `0x04` | Abort a stream immediately                   |
| HEADER  | `0x06` | Server response headers                      |

Full spec: [`docs/wire-format.md`](docs/wire-format.md)

---

## Repository layout

```
go/wsgrpc/              Go server library
  frame/                binary frame codec
  internal/conn/        WebSocket connection manager (stream multiplexer)
  internal/stream/      grpc.ServerStream implementation

packages/
  transport/            TypeScript frame codec + transport stubs
  protoc-gen-wsgrpc/    buf plugin — generates typed client stubs

proto/                  .proto definitions (wire format messages)
docs/                   specifications
```

---

## Go server library

```go
import "github.com/grpcws/wsgrpc"
```

Minimum Go version: **1.23** (uses generic `sync.Map`). Tested on Go 1.26.

Depends on [`github.com/coder/websocket`](https://github.com/coder/websocket) for WebSocket I/O and `google.golang.org/grpc` for the `ServerStream` interface.

```go
// Upgrade the HTTP request to a WebSocket and serve gRPC streams.
http.HandleFunc("/wsgrpc/", func(w http.ResponseWriter, r *http.Request) {
    ws, _ := websocket.Accept(w, r, nil)
    c := conn.New(ws, nil)
    c.OnStream = myDispatcher // Week 3: replaced by Server.ServeHTTP
    c.Serve(r.Context())
})
```

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

The `@grpcws/transport` package contains the frame codec and typed transport stubs. Full client implementation coming in Week 4.

### Running tests

```bash
cd packages/transport
pnpm test
```

---

## Code generation

Install [buf](https://buf.build) and run:

```bash
buf generate
```

This generates protobuf bindings and typed gRPC client stubs into `gen/ts/`.

---

## Roadmap

| Week | Milestone |
|------|-----------|
| 1    | Frame codec (Go + TypeScript) |
| 2    | WebSocket connection manager, stream multiplexer |
| 3    | gRPC dispatch layer, echo server example |
| 4    | TypeScript transport implementation |
| 5    | `protoc-gen-wsgrpc` code generator |
| 6    | `@grpcws/react` hooks |
| 7    | End-to-end integration, CI, publish prep |

---

## License

See [LICENSE](LICENSE).
