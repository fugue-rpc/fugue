# @fugue-rpc/transport

Browser and Node.js WebSocket transport for gRPC — all four call kinds from the browser.

Unlike gRPC-Web and Connect-ES, fugue supports **client-streaming and bidirectional streaming** from browsers. Those libraries cannot: the Fetch API buffers request bodies, preventing true client-initiated streams. Fugue uses a single long-lived WebSocket instead.

## Installation

```bash
npm install @fugue-rpc/transport
```

## Quick start

```ts
import { FugueTransport } from "@fugue-rpc/transport";

const transport = new FugueTransport("ws://localhost:8080/fugue/");
```

Create one transport per server connection. It manages a single WebSocket and multiplexes all streams over it.

## Unary

```ts
const result = await transport
  .openStream("/greet.v1.Greeter/SayHello")
  .unary(requestBytes, decode);
```

## Server streaming

```ts
const stream = transport
  .openStream("/greet.v1.Greeter/ListReplies")
  .serverStream(requestBytes, decode);

for await (const msg of stream) {
  console.log(msg);
}
```

## Client streaming

```ts
const cs = transport
  .openStream("/greet.v1.Greeter/CollectHellos")
  .clientStream(encode, decode);

cs.send(item1);
cs.send(item2);
const reply = await cs.closeAndReceive();
```

## Bidirectional streaming

```ts
const bidi = transport
  .openStream("/greet.v1.Chat/Chat")
  .bidiStream(encode, decode);

bidi.send(msg1);
bidi.send(msg2);
bidi.halfClose(); // signal end-of-client-stream

for await (const reply of bidi) {
  console.log(reply);
}
```

## Closing

```ts
transport.close(); // close the WebSocket connection
```

## Options

```ts
const transport = new FugueTransport("ws://...", {
  debug: true,      // log frame-level events
  protocols: [...], // WebSocket subprotocols
});
```

## Error handling

All calls throw `GrpcStatusError` on non-OK gRPC status:

```ts
import { GrpcStatusError } from "@fugue-rpc/transport";

try {
  await transport.openStream("/...").unary(req, decode);
} catch (err) {
  if (err instanceof GrpcStatusError) {
    console.log(err.code, err.message, err.metadata);
  }
}
```

## With generated clients

Use `protoc-gen-fugue` to generate typed wrappers from `.proto` files:

```ts
import { sayHello } from "@gen/greet/v1/greeter_fugue.js";

const reply = await sayHello(transport, { name: "world" });
```

## React

Use `@fugue-rpc/react` for hooks (`useUnary`, `useServerStream`, `useBidiStream`).

## Wire protocol

Fugue uses a custom binary framing protocol over WebSocket. See the [wire format spec](../../docs/wire-format.md) for the frame layout. The wire-level package identifier is `grpcws.frame.v1` — this is part of the protocol identity and is not renamed with the library.

## License

MIT
