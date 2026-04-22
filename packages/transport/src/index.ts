export type ConnectionState = "connecting" | "open" | "closed";

export type TransportOptions = {
  headers?: Record<string, string>;
};

export interface UnaryCall<Res> extends PromiseLike<Res> {
  cancel(): void;
}

export interface ServerStream<Res> extends AsyncIterable<Res> {
  cancel(): void;
}

export interface ClientStream<Req, Res> {
  send(request: Req): void;
  closeAndReceive(): Promise<Res>;
  cancel(): void;
}

export interface BidiStream<Req, Res> extends AsyncIterable<Res> {
  send(request: Req): void;
  halfClose(): void;
  cancel(): void;
}

// RawStream is the internal interface used by generated clients.
// Not part of the public API.
export interface RawStream {
  unary<Req, Res>(req: Req): UnaryCall<Res>;
  serverStream<Req, Res>(req: Req): ServerStream<Res>;
  clientStream<Req, Res>(): ClientStream<Req, Res>;
  bidiStream<Req, Res>(): BidiStream<Req, Res>;
}

export class WsGrpcTransport {
  constructor(_url: string, _options?: TransportOptions) {
    throw new Error("not implemented");
  }

  get state(): ConnectionState {
    throw new Error("not implemented");
  }

  get bufferedAmount(): number {
    throw new Error("not implemented");
  }

  close(): void {
    throw new Error("not implemented");
  }

  /** @internal */
  openStream(_method: string, _metadata?: Record<string, string>): RawStream {
    throw new Error("not implemented");
  }
}
