import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { decodeAll, encodeFrame, FrameType } from "./frame.js";
import { BeginPayloadSchema, EndPayloadSchema } from "./gen/frame_pb.js";
import { RawStream } from "./raw-stream.js";

export type ConnectionState = "connecting" | "open" | "closed";

export type TransportOptions = {
  /** Initial metadata sent with every request (e.g. authorization headers). */
  metadata?: Record<string, string>;
  /**
   * When true, every frame sent and received is logged via `console.debug`.
   * Useful for debugging CI failures where a test times out and you can't tell
   * which side stopped sending.
   */
  debug?: boolean;
  /** @internal — override WebSocket constructor for testing */
  _wsFactory?: (url: string) => WebSocket;
};

export class FugueTransport {
  private readonly _ws: WebSocket;
  private readonly _streams = new Map<number, RawStream>();
  private _nextStreamId = 1;
  private _state: ConnectionState = "connecting";
  private _pending: Uint8Array[] = [];
  private readonly _debug: boolean;

  constructor(url: string, options?: TransportOptions) {
    this._debug = options?.debug ?? false;
    const ws = options?._wsFactory
      ? options._wsFactory(url)
      : new WebSocket(url);
    ws.binaryType = "arraybuffer";

    ws.onopen = () => {
      this._state = "open";
      for (const frame of this._pending) this._ws.send(frame);
      this._pending = [];
    };

    ws.onmessage = (event: MessageEvent<ArrayBuffer>) => {
      try {
        // One WebSocket message may contain multiple coalesced frames.
        // Each frame is dispatched and logged individually (debug mode).
        for (const frame of decodeAll(new Uint8Array(event.data))) {
          this._handleFrame(frame);
        }
      } catch (err) {
        console.warn("fugue: malformed frame from server, closing transport", err);
        this._ws.close();
      }
    };

    ws.onclose = () => {
      this._state = "closed";
      this._pending = [];
      this._resetAll();
    };

    ws.onerror = () => {
      this._state = "closed";
      this._pending = [];
      this._resetAll();
    };

    this._ws = ws;
  }

  get state(): ConnectionState {
    return this._state;
  }

  /** Mirrors WebSocket.bufferedAmount — bytes enqueued but not yet flushed. */
  get bufferedAmount(): number {
    return this._ws.bufferedAmount;
  }

  close(): void {
    this._ws.close();
  }

  /**
   * Opens a new stream for the given gRPC method.
   * Sends a BEGIN frame immediately; the returned RawStream can be used to
   * create a typed call via .unary(), .serverStream(), .clientStream(), or
   * .bidiStream().
   * @internal — called by generated clients.
   */
  openStream(
    method: string,
    metadata?: Record<string, string>,
  ): RawStream {
    const streamId = this._nextStreamId++;
    const stream = new RawStream(
      streamId,
      (type, sid, payload) => this._write(type, sid, payload),
      (id) => this._streams.delete(id),
    );
    this._streams.set(streamId, stream);

    const beginPayload = create(BeginPayloadSchema, {
      method,
      metadata: metadata ?? {},
    });
    this._write(
      FrameType.BEGIN,
      streamId,
      toBinary(BeginPayloadSchema, beginPayload),
    );

    return stream;
  }

  private _write(
    type: number,
    streamId: number,
    payload: Uint8Array,
  ): void {
    if (this._debug) {
      console.debug("fugue ▲", { type, streamId, payloadBytes: payload.length });
    }
    const encoded = encodeFrame({
      type: type as (typeof FrameType)[keyof typeof FrameType],
      streamId,
      payload,
    });
    if (this._state === "connecting") {
      this._pending.push(encoded);
    } else {
      this._ws.send(encoded);
    }
  }

  private _handleFrame({ type, streamId, payload }: { type: number; streamId: number; payload: Uint8Array }): void {
    if (this._debug) {
      console.debug("fugue ▼", { type, streamId, payloadBytes: payload.length });
    }

    switch (type) {
      case FrameType.HEADER:
        // v0.1: server initial metadata is discarded on the client side.
        break;

      case FrameType.MSG: {
        this._streams.get(streamId)?._deliver(payload);
        break;
      }

      case FrameType.END: {
        const stream = this._streams.get(streamId);
        if (!stream) break;
        let statusCode = 0;
        let statusMessage = "";
        let trailers: Record<string, string> = {};
        if (payload.length > 0) {
          try {
            const ep = fromBinary(EndPayloadSchema, payload);
            statusCode = ep.statusCode;
            statusMessage = ep.statusMessage;
            trailers = ep.trailers;
          } catch {
            // Malformed EndPayload — treat as OK.
          }
        }
        stream._serverClose(statusCode, statusMessage, trailers);
        break;
      }

      case FrameType.RESET: {
        this._streams.get(streamId)?._reset();
        break;
      }

      default:
        // Unknown frame type — ignore per spec.
        break;
    }
  }

  private _resetAll(): void {
    for (const stream of this._streams.values()) {
      stream._reset();
    }
    this._streams.clear();
  }
}
