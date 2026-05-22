// Connection handler for one fugue WebSocket connection.
// Multiplexes gRPC streams per the wire protocol (docs/wire-format.md).

import type { WebSocket, RawData } from "ws";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { FrameType, encodeFrame, decodeAll } from "./frame.js";
import {
  BeginPayloadSchema, EndPayloadSchema, HeaderPayloadSchema,
} from "./proto.js";
import type { MethodEntry } from "./service.js";
import type {
  UnaryHandler, ServerStreamHandler, ClientStreamHandler, BidiHandler,
  UnaryServerCall, ServerStreamCall, ClientStreamCall, BidiCall,
} from "./service.js";

// gRPC status codes used internally.
const GRPC_OK                 = 0;
const GRPC_UNIMPLEMENTED      = 12;
const GRPC_INTERNAL           = 13;
const GRPC_RESOURCE_EXHAUSTED = 8;

/**
 * Called before every handler dispatch. Throw a gRPC status error to reject
 * the call (e.g. `throw Object.assign(new Error("UNAUTHENTICATED"), { code: 16 })`).
 * Runs before the method lookup, so it fires even for unimplemented methods.
 */
export type CallInterceptor = (
  ctx: { method: string; metadata: Readonly<Record<string, string>> }
) => void | Promise<void>;

export interface ConnOptions {
  /** Per-stream inbound message buffer depth. When full the stream is RESET. Default: 256. */
  recvBufSize?: number;
  /** Max concurrent streams per connection. 0 = unlimited (default). */
  maxStreams?: number;
  /** Pre-call hook for auth, logging, or metrics. Throw to reject a call. */
  interceptor?: CallInterceptor;
}

// Bounded async-push queue that backs the recv side of each stream.
// Single-consumer: only one [Symbol.asyncIterator] call per instance.
class RecvQueue {
  private readonly _buf: Buffer[] = [];
  private _waiter: (() => void) | null = null;
  private _closed = false;

  constructor(private readonly _capacity: number) {}

  // Returns true if accepted, false if buffer is full (caller should RESET stream).
  push(item: Buffer): boolean {
    if (this._closed) return true;
    if (this._buf.length >= this._capacity) return false;
    this._buf.push(item);
    const w = this._waiter;
    this._waiter = null;
    w?.();
    return true;
  }

  close(): void {
    if (this._closed) return;
    this._closed = true;
    const w = this._waiter;
    this._waiter = null;
    w?.();
  }

  [Symbol.asyncIterator](): AsyncIterator<Buffer> {
    return this._iterate();
  }

  private async *_iterate(): AsyncGenerator<Buffer> {
    while (true) {
      if (this._buf.length > 0) {
        yield this._buf.shift()!;
      } else if (this._closed) {
        return;
      } else {
        await new Promise<void>(r => { this._waiter = r; });
      }
    }
  }
}

interface StreamState {
  recvQueue: RecvQueue;
  headerSent: boolean;
  trailers: Record<string, string>;
  aborted: boolean;
}

export class FugueConn {
  private readonly _streams = new Map<number, StreamState>();
  private _highestId = 0;
  private _activeStreams = 0;
  private readonly _recvBufSize: number;
  private readonly _maxStreams: number;
  private readonly _interceptor: CallInterceptor | undefined;
  private _pendingFrames: Buffer[] = [];
  private _flushScheduled = false;
  private _connClosed = false;

  constructor(
    private readonly _ws: WebSocket,
    private readonly _lookup: (path: string) => MethodEntry | undefined,
    options?: ConnOptions,
  ) {
    this._recvBufSize = options?.recvBufSize ?? 256;
    this._maxStreams = options?.maxStreams ?? 0;
    this._interceptor = options?.interceptor;
  }

  serve(): void {
    this._ws.on("message", (data: RawData, isBinary: boolean) => {
      if (!isBinary) {
        this._ws.close(1002, "text frames are a protocol error");
        return;
      }
      let buf: Buffer;
      if (Buffer.isBuffer(data)) {
        buf = data;
      } else if (Array.isArray(data)) {
        buf = Buffer.concat(data as Buffer[]);
      } else {
        buf = Buffer.from(data as ArrayBuffer);
      }
      this._onMessage(buf);
    });

    this._ws.on("close", () => {
      this._connClosed = true;
      this._pendingFrames = [];
      for (const state of this._streams.values()) {
        state.aborted = true;
        state.recvQueue.close();
      }
    });

    this._ws.on("error", () => {
      // Errors surface as a close event; suppress the unhandled-error crash.
    });
  }

  private _onMessage(data: Buffer): void {
    let frames;
    try {
      frames = decodeAll(data);
    } catch (err) {
      this._ws.close(1002, String(err));
      return;
    }
    for (const frame of frames) {
      if (this._dispatch(frame)) return; // connection closed
    }
  }

  // Returns true if the connection was closed due to a protocol error.
  private _dispatch(frame: { type: number; streamId: number; payload: Buffer }): boolean {
    switch (frame.type) {
      case FrameType.BEGIN: return this._handleBEGIN(frame.streamId, frame.payload);
      case FrameType.MSG:   return this._handleMSG(frame.streamId, frame.payload);
      case FrameType.END:   return this._handleEND(frame.streamId);
      case FrameType.RESET: return this._handleRESET(frame.streamId);
      default:
        this._ws.close(1002, `unknown frame type 0x${frame.type.toString(16).padStart(2, "0")}`);
        return true;
    }
  }

  private _handleBEGIN(streamId: number, payload: Buffer): boolean {
    if (streamId === 0) {
      this._ws.close(1002, "stream_id 0 is reserved");
      return true;
    }
    if (streamId <= this._highestId) {
      this._ws.close(1002, `non-monotonic stream_id: ${streamId} ≤ highest ${this._highestId}`);
      return true;
    }
    this._highestId = streamId;

    let method = "";
    let metadata: Record<string, string> = {};
    if (payload.length > 0) {
      try {
        const bp = fromBinary(BeginPayloadSchema, payload);
        method = bp.method;
        metadata = bp.metadata;
      } catch {
        this._ws.close(1002, "bad BeginPayload");
        return true;
      }
    }

    if (this._maxStreams > 0 && this._activeStreams >= this._maxStreams) {
      this._sendFrame(
        FrameType.END, streamId,
        encodeEndPayload(GRPC_RESOURCE_EXHAUSTED, "too many concurrent streams", {}),
      );
      return false;
    }

    const state: StreamState = {
      recvQueue: new RecvQueue(this._recvBufSize),
      headerSent: false,
      trailers: {},
      aborted: false,
    };
    this._streams.set(streamId, state);
    this._activeStreams++;

    this._dispatchHandler(streamId, method, metadata, state).catch(() => {});
    return false;
  }

  private _handleMSG(streamId: number, payload: Buffer): boolean {
    const state = this._streams.get(streamId);
    if (!state) return false; // in-flight race after stream closed — silently drop
    if (!state.recvQueue.push(payload)) {
      // Buffer full: reset stream, but keep the connection alive (stream independence).
      state.aborted = true;
      state.recvQueue.close();
      this._streams.delete(streamId);
      this._activeStreams--;
      this._sendFrame(
        FrameType.END, streamId,
        encodeEndPayload(GRPC_RESOURCE_EXHAUSTED, "inbound message buffer full", {}),
      );
    }
    return false;
  }

  private _handleEND(streamId: number): boolean {
    const state = this._streams.get(streamId);
    if (!state) return false;
    state.recvQueue.close();
    return false;
  }

  private _handleRESET(streamId: number): boolean {
    const state = this._streams.get(streamId);
    if (!state) return false; // spec §4.4: silently drop RESET for unknown stream
    state.aborted = true;
    state.recvQueue.close();
    this._streams.delete(streamId);
    this._activeStreams--;
    return false;
  }

  private async _dispatchHandler(
    streamId: number,
    method: string,
    metadata: Record<string, string>,
    state: StreamState,
  ): Promise<void> {
    if (this._interceptor) {
      try {
        await this._interceptor({ method, metadata });
      } catch (err) {
        this._cleanup(streamId);
        const { code, message } = extractGrpcStatus(err);
        this._sendFrame(FrameType.END, streamId, encodeEndPayload(code, message, {}));
        return;
      }
    }

    const entry = this._lookup(method);
    if (!entry) {
      this._cleanup(streamId);
      this._sendFrame(
        FrameType.END, streamId,
        encodeEndPayload(GRPC_UNIMPLEMENTED, `unknown method ${method}`, {}),
      );
      return;
    }

    const sendHeader = (headers: Record<string, string>): void => {
      if (state.headerSent || state.aborted) return;
      state.headerSent = true;
      this._sendFrame(
        FrameType.HEADER, streamId,
        toBinary(HeaderPayloadSchema, create(HeaderPayloadSchema, { headers })),
      );
    };

    const autoHeader = (): void => {
      if (!state.headerSent) sendHeader({});
    };

    const setTrailer = (trailers: Record<string, string>): void => {
      Object.assign(state.trailers, trailers);
    };

    const write = (response: unknown): void => {
      if (state.aborted) return;
      autoHeader();
      this._sendFrame(FrameType.MSG, streamId, entry.serialize(response as never));
    };

    const callBase = { metadata, sendHeader, setTrailer };

    let handlerError: unknown = null;
    try {
      switch (entry.kind) {
        case "unary": {
          const request = await this._recvFirst(state.recvQueue, entry.deserialize);
          if (state.aborted) break;
          const call: UnaryServerCall<unknown> = { ...callBase, request };
          const response = await (entry.handler as UnaryHandler<unknown, unknown>)(call);
          if (!state.aborted) {
            autoHeader();
            this._sendFrame(FrameType.MSG, streamId, entry.serialize(response));
          }
          break;
        }
        case "server_stream": {
          const request = await this._recvFirst(state.recvQueue, entry.deserialize);
          if (state.aborted) break;
          const call: ServerStreamCall<unknown, unknown> = { ...callBase, request, write };
          await (entry.handler as ServerStreamHandler<unknown, unknown>)(call);
          break;
        }
        case "client_stream": {
          const iter = makeIter(state.recvQueue, entry.deserialize);
          const call: ClientStreamCall<unknown> = {
            ...callBase,
            [Symbol.asyncIterator]: () => iter,
          };
          const response = await (entry.handler as ClientStreamHandler<unknown, unknown>)(call);
          if (!state.aborted) {
            autoHeader();
            this._sendFrame(FrameType.MSG, streamId, entry.serialize(response));
          }
          break;
        }
        case "bidi_stream": {
          const iter = makeIter(state.recvQueue, entry.deserialize);
          const call: BidiCall<unknown, unknown> = {
            ...callBase,
            write,
            [Symbol.asyncIterator]: () => iter,
          };
          await (entry.handler as BidiHandler<unknown, unknown>)(call);
          break;
        }
      }
    } catch (err) {
      handlerError = err;
    }

    this._cleanup(streamId);

    if (state.aborted) return;

    if (handlerError != null) {
      const { code, message } = extractGrpcStatus(handlerError);
      this._sendFrame(FrameType.END, streamId, encodeEndPayload(code, message, state.trailers));
    } else {
      this._sendFrame(FrameType.END, streamId, encodeEndPayload(GRPC_OK, "", state.trailers));
    }
  }

  private _cleanup(streamId: number): void {
    if (this._streams.delete(streamId)) {
      this._activeStreams--;
    }
  }

  private async _recvFirst(queue: RecvQueue, deserialize: (b: Buffer) => unknown): Promise<unknown> {
    for await (const buf of queue) {
      return deserialize(buf);
    }
    throw Object.assign(new Error("stream closed before request arrived"), { code: GRPC_INTERNAL });
  }

  private _sendFrame(type: number, streamId: number, payload: Uint8Array): void {
    if (this._connClosed) return;
    this._pendingFrames.push(encodeFrame(type, streamId, payload));
    if (!this._flushScheduled) {
      this._flushScheduled = true;
      queueMicrotask(() => { this._flush(); });
    }
  }

  private _flush(): void {
    this._flushScheduled = false;
    const frames = this._pendingFrames;
    if (frames.length === 0 || this._connClosed) return;
    this._pendingFrames = [];
    try {
      this._ws.send(frames.length === 1 ? frames[0] : Buffer.concat(frames));
    } catch {
      // WebSocket already closed — ignore.
    }
  }
}

function makeIter<T>(queue: RecvQueue, deserialize: (b: Buffer) => T): AsyncIterator<T> {
  return (async function* () {
    for await (const buf of queue) {
      yield deserialize(buf);
    }
  })();
}

function encodeEndPayload(code: number, message: string, trailers: Record<string, string>): Uint8Array {
  return toBinary(EndPayloadSchema, create(EndPayloadSchema, {
    statusCode: code,
    statusMessage: message,
    trailers,
  }));
}

function extractGrpcStatus(err: unknown): { code: number; message: string } {
  if (err != null && typeof err === "object") {
    const e = err as Record<string, unknown>;
    if (typeof e["code"] === "number" && typeof e["message"] === "string") {
      return { code: e["code"] as number, message: e["message"] as string };
    }
  }
  return { code: GRPC_INTERNAL, message: String(err) };
}
