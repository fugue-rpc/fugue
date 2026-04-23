import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { decodeFrame, encodeFrame, FrameType } from "./frame.js";
import { BeginPayloadSchema, EndPayloadSchema } from "./gen/frame_pb.js";
import { RawStream } from "./raw-stream.js";

export type ConnectionState = "connecting" | "open" | "closed";

export type TransportOptions = {
  /** Initial metadata sent with every request (e.g. authorization headers). */
  metadata?: Record<string, string>;
  /** @internal — override WebSocket constructor for testing */
  _wsFactory?: (url: string) => WebSocket;
};

export class WsGrpcTransport {
  private readonly _ws: WebSocket;
  private readonly _streams = new Map<number, RawStream>();
  private _nextStreamId = 1;
  private _state: ConnectionState = "connecting";

  constructor(url: string, options?: TransportOptions) {
    const ws = options?._wsFactory
      ? options._wsFactory(url)
      : new WebSocket(url);
    ws.binaryType = "arraybuffer";

    ws.onopen = () => {
      this._state = "open";
    };

    ws.onmessage = (event: MessageEvent<ArrayBuffer>) => {
      try {
        this._handleFrame(new Uint8Array(event.data));
      } catch {
        // Malformed frame from server — ignore; connection stays alive.
      }
    };

    ws.onclose = () => {
      this._state = "closed";
      this._resetAll();
    };

    ws.onerror = () => {
      this._state = "closed";
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
    const encoded = encodeFrame({
      type: type as (typeof FrameType)[keyof typeof FrameType],
      streamId,
      payload,
    });
    this._ws.send(encoded);
  }

  private _handleFrame(buf: Uint8Array): void {
    const { type, streamId, payload } = decodeFrame(buf);

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
