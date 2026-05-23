# @fugue-rpc/angular

Angular services for [fugue](https://github.com/fugue-rpc/fugue) — gRPC over WebSocket for browsers.

Wraps `@fugue-rpc/transport` with Angular-idiomatic injectable services backed by **Signals** and **RxJS Observables** for all four gRPC call kinds.

## Installation

```bash
npm install @fugue-rpc/angular @fugue-rpc/transport
# peer deps
npm install @angular/core rxjs
```

Requires Angular ≥ 17 (signals API).

## Setup

Register the transport in your app config:

```ts
// app.config.ts
import { ApplicationConfig } from "@angular/core";
import { FugueTransport } from "@fugue-rpc/transport";
import { provideFugue } from "@fugue-rpc/angular";

const transport = new FugueTransport("ws://localhost:8080/fugue/");

export const appConfig: ApplicationConfig = {
  providers: [
    provideFugue(transport),
  ],
};
```

## FugueUnaryService

```ts
import { Component, inject } from "@angular/core";
import { FUGUE_TRANSPORT, FugueUnaryService } from "@fugue-rpc/angular";

@Component({
  template: `
    <button (click)="greet()">Greet</button>
    @if (svc.state().status === 'loading') { <p>Loading…</p> }
    @if (svc.state().status === 'success') { <p>{{ svc.state().data }}</p> }
    @if (svc.state().status === 'error')   { <p>Error: {{ svc.state().error.message }}</p> }
  `,
  providers: [FugueUnaryService],
})
export class GreetComponent {
  private transport = inject(FUGUE_TRANSPORT);
  svc = inject<FugueUnaryService<Uint8Array, string>>(FugueUnaryService);

  greet() {
    this.svc.execute(
      (req) => this.transport.openStream("/greet.v1.Greeter/SayHello").unary(req, decode),
      encode({ name: "world" }),
    );
  }
}
```

State machine: `idle → loading → success | error`. Call `reset()` to return to idle.

Both `svc.state()` (Signal) and `svc.state$` (Observable) are available.

## FugueServerStreamService

```ts
import { FugueServerStreamService } from "@fugue-rpc/angular";

@Component({
  template: `
    <button (click)="start()">Stream</button>
    <button (click)="svc.reset()">Stop</button>
    @for (msg of svc.state().messages; track $index) { <p>{{ msg }}</p> }
    @if (svc.state().status === 'done') { <p>Done</p> }
  `,
  providers: [FugueServerStreamService],
})
export class StreamComponent {
  private transport = inject(FUGUE_TRANSPORT);
  svc = inject<FugueServerStreamService<Uint8Array, string>>(FugueServerStreamService);

  start() {
    this.svc.start(
      (req) => this.transport.openStream("/greet.v1.Greeter/ListReplies").serverStream(req, decode),
      encode({ name: "world" }),
    );
  }
}
```

State machine: `idle → streaming → done | error`. `state().messages` accumulates all received messages.

> **Note:** Each message triggers a signal update with a shallow copy of the messages array. For high-frequency streams, use `transport.openStream()` directly.

## FugueBidiStreamService

```ts
import { FugueBidiStreamService } from "@fugue-rpc/angular";

@Component({
  template: `
    <button (click)="open()">Connect</button>
    <button (click)="send()">Send</button>
    <button (click)="svc.halfClose()">Done sending</button>
    <button (click)="svc.cancel()">Disconnect</button>
    @for (msg of svc.state().messages; track $index) { <p>{{ msg }}</p> }
  `,
  providers: [FugueBidiStreamService],
})
export class ChatComponent {
  private transport = inject(FUGUE_TRANSPORT);
  svc = inject<FugueBidiStreamService<string, string>>(FugueBidiStreamService);

  open() {
    this.svc.open(
      () => this.transport.openStream("/chat.v1.Chat/Connect").bidiStream(encode, decode),
    );
  }

  send() {
    this.svc.send("hello");
  }
}
```

State machine: `idle → open → done | error`. `send(req)` any time the stream is open. `halfClose()` signals end-of-client-stream. `cancel()` closes immediately. `reset()` returns to idle.

`open(factory, initialRequest?)` is a no-op if already open — safe to call on every interaction.

## Lifecycle

All services inject `DestroyRef` and cancel their active stream automatically when the component is destroyed. No manual cleanup needed.

## RxJS interop

Every service exposes `state$: Observable<...>` alongside the Signal, powered by `toObservable`. Use it with `async` pipe or RxJS pipelines:

```ts
this.svc.state$.pipe(
  filter(s => s.status === 'success'),
).subscribe(s => console.log(s.data));
```

## License

MIT
