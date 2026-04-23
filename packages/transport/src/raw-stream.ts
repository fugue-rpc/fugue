import { FrameType } from "./frame.js";

// --- Public interfaces ---

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

// --- Error type ---

export class GrpcStatusError extends Error {
  constructor(
    readonly code: number,
    override readonly message: string,
    readonly trailers: Record<string, string>,
  ) {
    super(message);
    this.name = "GrpcStatusError";
  }
}

// --- Internal queue types ---

type QueueEntry =
  | { tag: "msg"; payload: Uint8Array }
  | { tag: "done" }
  | { tag: "error"; error: GrpcStatusError };

type Waiter = {
  resolve: (result: IteratorResult<Uint8Array, undefined>) => void;
  reject: (err: unknown) => void;
};

type WriteFn = (type: number, streamId: number, payload: Uint8Array) => void;

export type StreamState =
  | "open"
  | "half-closed-local"
  | "half-closed-remote"
  | "closed";

// --- RawStream ---

/**
 * Manages one logical gRPC stream multiplexed over the WebSocket.
 * Created by WsGrpcTransport.openStream(). Used by generated clients via the
 * unary/serverStream/clientStream/bidiStream factory methods.
 */
export class RawStream {
  private _state: StreamState = "open";
  private _queue: QueueEntry[] = [];
  private _waiter: Waiter | null = null;

  constructor(
    readonly streamId: number,
    private readonly _write: WriteFn,
    private readonly _onDone: (id: number) => void,
  ) {}

  get state(): StreamState {
    return this._state;
  }

  // --- Wire-level methods (used by call factories below) ---

  /** Send a MSG frame. Throws synchronously if the stream can no longer send. */
  _sendMsg(payload: Uint8Array): void {
    if (this._state === "half-closed-local" || this._state === "closed") {
      throw new Error("wsgrpc: stream closed");
    }
    this._write(FrameType.MSG, this.streamId, payload);
  }

  /** Send a client END frame (half-close). No-op if already half-closed or closed. */
  _sendEnd(): void {
    if (this._state === "open") {
      this._write(FrameType.END, this.streamId, new Uint8Array(0));
      this._state = "half-closed-local";
    } else if (this._state === "half-closed-remote") {
      this._write(FrameType.END, this.streamId, new Uint8Array(0));
      this._close(false); // both sides closed normally — keep queue for draining
    }
  }

  /** Send a RESET frame and abort. Idempotent if already closed. */
  cancel(): void {
    if (this._state === "closed") return;
    this._write(FrameType.RESET, this.streamId, new Uint8Array(0));
    this._close(true); // discard pending messages
  }

  // --- Transport callbacks ---

  /** Called by transport when a MSG frame arrives for this stream. */
  _deliver(payload: Uint8Array): void {
    if (this._state === "closed" || this._state === "half-closed-remote") return;
    this._push({ tag: "msg", payload });
  }

  /** Called by transport when an END frame arrives (server half-close). */
  _serverClose(
    statusCode: number,
    statusMessage: string,
    trailers: Record<string, string>,
  ): void {
    if (this._state === "closed" || this._state === "half-closed-remote") return;
    const wasHalfClosedLocal = this._state === "half-closed-local";
    this._state = "half-closed-remote";

    if (statusCode !== 0) {
      this._push({
        tag: "error",
        error: new GrpcStatusError(statusCode, statusMessage, trailers),
      });
    } else {
      this._push({ tag: "done" });
    }

    if (wasHalfClosedLocal) {
      this._close(false); // both sides closed — keep queue so messages drain first
    }
  }

  /** Called by transport on RESET frame or WebSocket close. */
  _reset(): void {
    if (this._state === "closed") return;
    this._close(true); // discard pending messages
  }

  // --- Async iterator ---

  [Symbol.asyncIterator](): AsyncIterator<Uint8Array, undefined, undefined> {
    // Each call to [Symbol.asyncIterator] creates an independent iterator cursor.
    // RawStream only supports one concurrent reader (single consumer model).
    return this._makeIterator();
  }

  private _makeIterator(): AsyncIterator<Uint8Array, undefined, undefined> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    return {
      next(): Promise<IteratorResult<Uint8Array, undefined>> {
        if (self._queue.length > 0) {
          return self._dequeue();
        }
        if (self._state === "closed" || self._state === "half-closed-remote") {
          return Promise.resolve({ value: undefined, done: true });
        }
        return new Promise<IteratorResult<Uint8Array, undefined>>(
          (resolve, reject) => {
            self._waiter = { resolve, reject };
          },
        );
      },
      return(): Promise<IteratorResult<Uint8Array, undefined>> {
        // Called when a for-await loop exits early (break/return/throw in body).
        self.cancel();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }

  private _dequeue(): Promise<IteratorResult<Uint8Array, undefined>> {
    const entry = this._queue.shift()!;
    if (entry.tag === "msg") {
      return Promise.resolve({ value: entry.payload, done: false });
    }
    if (entry.tag === "done") {
      return Promise.resolve({ value: undefined, done: true });
    }
    // tag === 'error'
    return Promise.reject(entry.error);
  }

  private _push(entry: QueueEntry): void {
    if (this._waiter) {
      const { resolve, reject } = this._waiter;
      this._waiter = null;
      if (entry.tag === "msg") {
        resolve({ value: entry.payload, done: false });
      } else if (entry.tag === "done") {
        resolve({ value: undefined, done: true });
      } else {
        reject(entry.error);
      }
    } else {
      this._queue.push(entry);
    }
  }

  /**
   * Transition to CLOSED.
   * @param clearQueue - true for abort (cancel/reset): discard pending msgs.
   *                     false for normal close: leave queue so consumer drains it.
   */
  private _close(clearQueue: boolean): void {
    this._state = "closed";
    if (this._waiter) {
      const { resolve } = this._waiter;
      this._waiter = null;
      resolve({ value: undefined, done: true });
    }
    if (clearQueue) this._queue = [];
    this._onDone(this.streamId);
  }

  // --- Typed call factories ---

  /**
   * Unary RPC: send one request, receive one response.
   * @param reqBytes - proto-encoded request
   * @param decode   - proto decoder for the response type
   */
  unary<Res>(
    reqBytes: Uint8Array,
    decode: (bytes: Uint8Array) => Res,
  ): UnaryCall<Res> {
    return new UnaryCallImpl<Res>(this, reqBytes, decode);
  }

  /**
   * Server-streaming RPC: send one request, receive a stream of responses.
   */
  serverStream<Res>(
    reqBytes: Uint8Array,
    decode: (bytes: Uint8Array) => Res,
  ): ServerStream<Res> {
    return new ServerStreamImpl<Res>(this, reqBytes, decode);
  }

  /**
   * Client-streaming RPC: send a stream of requests, receive one response.
   */
  clientStream<Req, Res>(
    encode: (req: Req) => Uint8Array,
    decode: (bytes: Uint8Array) => Res,
  ): ClientStream<Req, Res> {
    return new ClientStreamImpl<Req, Res>(this, encode, decode);
  }

  /**
   * Bidi-streaming RPC: send and receive independently.
   */
  bidiStream<Req, Res>(
    encode: (req: Req) => Uint8Array,
    decode: (bytes: Uint8Array) => Res,
  ): BidiStream<Req, Res> {
    return new BidiStreamImpl<Req, Res>(this, encode, decode);
  }
}

// --- Concrete call implementations ---

class UnaryCallImpl<Res> implements UnaryCall<Res> {
  private readonly _stream: RawStream;
  private readonly _promise: Promise<Res>;

  constructor(
    stream: RawStream,
    reqBytes: Uint8Array,
    decode: (bytes: Uint8Array) => Res,
  ) {
    this._stream = stream;
    stream._sendMsg(reqBytes);
    stream._sendEnd();

    const iter = stream[Symbol.asyncIterator]();
    this._promise = iter.next().then((result) => {
      if (result.done) {
        throw new Error("wsgrpc: no response message received");
      }
      return decode(result.value);
    });
  }

  then<TResult1 = Res, TResult2 = never>(
    onfulfilled?:
      | ((value: Res) => TResult1 | PromiseLike<TResult1>)
      | null
      | undefined,
    onrejected?:
      | ((reason: any) => TResult2 | PromiseLike<TResult2>) // eslint-disable-line @typescript-eslint/no-explicit-any
      | null
      | undefined,
  ): PromiseLike<TResult1 | TResult2> {
    return this._promise.then<TResult1, TResult2>(onfulfilled, onrejected);
  }

  cancel(): void {
    this._stream.cancel();
  }
}

class ServerStreamImpl<Res> implements ServerStream<Res> {
  private readonly _stream: RawStream;
  private readonly _decode: (bytes: Uint8Array) => Res;

  constructor(
    stream: RawStream,
    reqBytes: Uint8Array,
    decode: (bytes: Uint8Array) => Res,
  ) {
    this._stream = stream;
    this._decode = decode;
    stream._sendMsg(reqBytes);
    stream._sendEnd();
  }

  [Symbol.asyncIterator](): AsyncIterator<Res, undefined, undefined> {
    const rawIter = this._stream[Symbol.asyncIterator]();
    const decode = this._decode;
    return {
      next(): Promise<IteratorResult<Res, undefined>> {
        return rawIter.next().then((r) =>
          r.done
            ? { value: undefined, done: true }
            : { value: decode(r.value), done: false },
        );
      },
      return(): Promise<IteratorResult<Res, undefined>> {
        return rawIter.return!().then(() => ({ value: undefined, done: true as const }));
      },
    };
  }

  cancel(): void {
    this._stream.cancel();
  }
}

class ClientStreamImpl<Req, Res> implements ClientStream<Req, Res> {
  private readonly _stream: RawStream;
  private readonly _encode: (req: Req) => Uint8Array;
  private readonly _decode: (bytes: Uint8Array) => Res;

  constructor(
    stream: RawStream,
    encode: (req: Req) => Uint8Array,
    decode: (bytes: Uint8Array) => Res,
  ) {
    this._stream = stream;
    this._encode = encode;
    this._decode = decode;
  }

  send(req: Req): void {
    this._stream._sendMsg(this._encode(req));
  }

  closeAndReceive(): Promise<Res> {
    this._stream._sendEnd();
    const iter = this._stream[Symbol.asyncIterator]();
    return iter.next().then((result) => {
      if (result.done) {
        throw new Error("wsgrpc: no response message received");
      }
      return this._decode(result.value);
    });
  }

  cancel(): void {
    this._stream.cancel();
  }
}

class BidiStreamImpl<Req, Res> implements BidiStream<Req, Res> {
  private readonly _stream: RawStream;
  private readonly _encode: (req: Req) => Uint8Array;
  private readonly _decode: (bytes: Uint8Array) => Res;

  constructor(
    stream: RawStream,
    encode: (req: Req) => Uint8Array,
    decode: (bytes: Uint8Array) => Res,
  ) {
    this._stream = stream;
    this._encode = encode;
    this._decode = decode;
  }

  send(req: Req): void {
    this._stream._sendMsg(this._encode(req));
  }

  halfClose(): void {
    this._stream._sendEnd();
  }

  cancel(): void {
    this._stream.cancel();
  }

  [Symbol.asyncIterator](): AsyncIterator<Res, undefined, undefined> {
    const rawIter = this._stream[Symbol.asyncIterator]();
    const decode = this._decode;
    return {
      next(): Promise<IteratorResult<Res, undefined>> {
        return rawIter.next().then((r) =>
          r.done
            ? { value: undefined, done: true }
            : { value: decode(r.value), done: false },
        );
      },
      return(): Promise<IteratorResult<Res, undefined>> {
        return rawIter.return!().then(() => ({ value: undefined, done: true as const }));
      },
    };
  }
}
