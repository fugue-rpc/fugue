# @fugue-rpc/react

React hooks for [fugue](https://github.com/fugue-rpc/fugue) — gRPC over WebSocket for browsers.

Wraps `@fugue-rpc/transport` with hooks for all four gRPC call kinds: `useUnary`, `useServerStream`, `useBidiStream`.

## Installation

```bash
npm install @fugue-rpc/react @fugue-rpc/transport
# peer dep
npm install react
```

## Setup

Wrap your app in `<FugueProvider>` with a single shared `FugueTransport`:

```tsx
import { FugueTransport } from "@fugue-rpc/transport";
import { FugueProvider } from "@fugue-rpc/react";

const transport = new FugueTransport("ws://localhost:8080/fugue/");

function App() {
  return (
    <FugueProvider transport={transport}>
      <YourApp />
    </FugueProvider>
  );
}
```

## useUnary

```tsx
import { useTransport, useUnary } from "@fugue-rpc/react";

function GreetButton() {
  const transport = useTransport();

  const { state, execute, reset } = useUnary((req: Uint8Array) =>
    transport.openStream("/greet.v1.Greeter/SayHello").unary(req, decode)
  );

  if (state.status === "loading") return <p>Loading…</p>;
  if (state.status === "error")   return <p>Error: {state.error.message}</p>;
  if (state.status === "success") return <p>{state.data}</p>;

  return <button onClick={() => execute(encode({ name: "world" }))}>Greet</button>;
}
```

State machine: `idle → loading → success | error`. Call `reset()` to return to idle and cancel any in-flight call.

## useServerStream

```tsx
import { useTransport, useServerStream } from "@fugue-rpc/react";

function StreamList() {
  const transport = useTransport();

  const { state, start, reset } = useServerStream((req: Uint8Array) =>
    transport.openStream("/greet.v1.Greeter/ListReplies").serverStream(req, decode)
  );

  return (
    <div>
      <button onClick={() => start(encode({ name: "world" }))}>Stream</button>
      <button onClick={reset}>Stop</button>
      <ul>
        {(state.status === "streaming" || state.status === "done") &&
          state.messages.map((m, i) => <li key={i}>{m}</li>)}
      </ul>
      {state.status === "done" && <p>Done ({state.messages.length} messages)</p>}
    </div>
  );
}
```

State machine: `idle → streaming → done | error`. Accumulated messages are available on `state.messages` in all non-idle states.

> **Note:** Each incoming message triggers a re-render with a shallow copy of the messages array. For high-frequency streams, use `transport.openStream()` directly and manage state yourself.

## useBidiStream

```tsx
import { useTransport, useBidiStream } from "@fugue-rpc/react";

function Chat() {
  const transport = useTransport();
  const [input, setInput] = React.useState("");

  const { state, open, send, halfClose, cancel } = useBidiStream(() =>
    transport.openStream("/chat.v1.Chat/Connect").bidiStream(encode, decode)
  );

  return (
    <div>
      <button onClick={open} disabled={state.status === "open"}>Connect</button>
      <button onClick={() => { send(encode(input)); setInput(""); }}>Send</button>
      <button onClick={halfClose}>Done sending</button>
      <button onClick={cancel}>Disconnect</button>
      <ul>
        {(state.status === "open" || state.status === "done") &&
          state.messages.map((m, i) => <li key={i}>{m}</li>)}
      </ul>
    </div>
  );
}
```

State machine: `idle → open → done | error`. Call `send(req)` any time the stream is open. `halfClose()` signals end-of-client-stream (server may still send). `cancel()` closes immediately. Call `reset()` to return to idle.

`open(initialRequest?)` is a no-op if the stream is already open — safe to call on every render.

## With generated clients

Use `protoc-gen-fugue` to generate typed client helpers, then pass them to the hooks:

```tsx
import { useUnary, useTransport } from "@fugue-rpc/react";
import { sayHello } from "@gen/greet/v1/greeter_fugue.js";

function Greet() {
  const transport = useTransport();
  const { state, execute } = useUnary((req) => sayHello(transport, req));
  // ...
}
```

## TypeScript

All hooks are fully generic. TypeScript infers `Req` and `Res` from the call factory you provide — no manual type parameters needed in most cases.

## License

MIT
