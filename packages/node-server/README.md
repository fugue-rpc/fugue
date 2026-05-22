# @fugue-rpc/node-server

Node.js gRPC-over-WebSocket server — accepts all four gRPC RPC kinds (unary, server-streaming, client-streaming, bidirectional) from browser clients over a single long-lived WebSocket connection.

## Why

gRPC-Web and Connect-ES support unary and server-streaming calls from browsers. They cannot support client-streaming or bidi-streaming because the browser Fetch API buffers the full request body before sending. WebSocket has no such limitation. `@fugue-rpc/node-server` exposes all four RPC kinds to `@fugue-rpc/transport` browser clients.

## Installation

```bash
npm install @fugue-rpc/node-server ws
```

## Quick start

```typescript
import { createServer } from "node:http";
import { FugueServer } from "@fugue-rpc/node-server";
import type { ServiceDefinition } from "@fugue-rpc/node-server";

// Define your service. Duck-compatible with @grpc/grpc-js ServiceDefinition.
const GreeterService = {
  sayHello: {
    path: "/myapp.v1.Greeter/SayHello",
    requestStream: false,
    responseStream: false,
    requestDeserialize: (buf: Buffer) => buf.toString("utf8"),
    responseSerialize: (val: string) => Buffer.from(val, "utf8"),
  },
} satisfies ServiceDefinition;

const srv = new FugueServer({ origins: ["https://app.example.com"] });
srv.addService(GreeterService, {
  sayHello: async (call) => `hello, ${call.request}`,
});

const httpServer = createServer();
srv.attach(httpServer, "/fugue/");
httpServer.listen(8080);
```

## API

### `new FugueServer(options?)`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `origins` | `string \| string[]` | — | Allowed `Origin` values. `"*"` allows all. When absent, browser clients (those sending an `Origin` header) are rejected; non-browser clients are accepted. |
| `recvBufSize` | `number` | `256` | Per-stream inbound message buffer depth. A stream whose buffer fills is reset with `RESOURCE_EXHAUSTED`. |
| `maxStreams` | `number` | `0` | Max concurrent streams per connection. `0` = unlimited. |
| `interceptor` | `CallInterceptor` | — | Pre-call hook for auth, logging, or metrics. Throw to reject. See [Interceptors](#interceptors). |

### `srv.addService(definition, implementation): this`

Register service methods. Returns `this` for chaining. `definition` is structurally compatible with the `ServiceDefinition` type emitted by `@grpc/grpc-js`'s protoc plugin, so generated descriptor objects can be passed directly.

### `srv.attach(httpServer, path?): this`

Hook into an existing `http.Server` or `https.Server` via the `upgrade` event. WebSocket upgrade requests whose URL starts with `path` are handled; all others have their socket immediately destroyed (no leak).

### Interceptors

Pass an `interceptor` to `FugueServer` (or `FugueConn`) to add pre-call logic — auth, logging, metrics — across every stream. The interceptor fires before the method lookup, so it runs even for unimplemented methods.

```typescript
import { FugueServer, type CallInterceptor } from "@fugue-rpc/node-server";

const authInterceptor: CallInterceptor = async ({ method, metadata }) => {
  if (!isValidToken(metadata["authorization"])) {
    throw Object.assign(new Error("UNAUTHENTICATED"), { code: 16 });
  }
};

const srv = new FugueServer({ origins: "*", interceptor: authInterceptor });
```

If the interceptor throws, the call receives an `END` frame with that status and the handler never runs. Errors without a `code` become `INTERNAL` (13). Multiple concerns can be composed in a single function or chained manually.

### Handler shapes

```typescript
// Unary: return a single response value
type UnaryHandler<Req, Res> = (call: UnaryServerCall<Req>) => Res | Promise<Res>;

// Server-streaming: call write() any number of times, then return
type ServerStreamHandler<Req, Res> = (call: ServerStreamCall<Req, Res>) => Promise<void>;

// Client-streaming: async-iterate incoming requests, return one response
type ClientStreamHandler<Req, Res> = (call: ClientStreamCall<Req>) => Res | Promise<Res>;

// Bidirectional: async-iterate requests and call write() concurrently
type BidiHandler<Req, Res> = (call: BidiCall<Req, Res>) => Promise<void>;
```

All call objects expose:

| Member | Description |
|--------|-------------|
| `call.metadata` | Key/value pairs from the `BEGIN` frame |
| `call.sendHeader(headers)` | Send a HEADER frame (auto-sent before the first message if not called) |
| `call.setTrailer(trailers)` | Add trailers merged into the closing `END` frame |

### gRPC error status

Throw an object with a numeric `code` and string `message` to send a specific gRPC status code:

```typescript
throw Object.assign(new Error("not found"), { code: 5 }); // NOT_FOUND
```

Unhandled errors become `INTERNAL` (code 13).

## Example

`examples/node-echo-server/` is a complete echo server implementing all four RPC kinds on `:8080/fugue/`. Run with:

```bash
pnpm start
```

## Browser client

Use [`@fugue-rpc/transport`](../transport/) to call this server from a browser or Node.js client, and [`@fugue-rpc/react`](../react/) for React hooks.

## @grpc/grpc-js compatibility

`ServiceDefinition` is structurally compatible with the output of `@grpc/grpc-js`'s protoc plugin. Pass the generated definition object directly to `addService()` with no adapter needed.

## Wire protocol

The server implements the fugue framing protocol: a 9-byte binary header per frame (1-byte type, 4-byte stream ID, 4-byte payload length) with five frame types (`BEGIN`, `MSG`, `END`, `RESET`, `HEADER`). Full spec: `docs/wire-format.md`.
