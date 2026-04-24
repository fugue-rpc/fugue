import { describe, expect, it } from "vitest";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { encodeFrame, FrameType } from "./frame.js";
import {
  BeginPayloadSchema,
  EndPayloadSchema,
} from "./gen/frame_pb.js";
import { GrpcStatusError, WsGrpcTransport } from "./index.js";

// ── Fake WebSocket ────────────────────────────────────────────────────────────
// Mimics the browser WebSocket API without any real network I/O.

class FakeWebSocket {
  binaryType: string = "arraybuffer";
  // Real WebSockets transition 0→1→2→3. The transport tracks its own _state
  // and never reads readyState, so the fake holds it at OPEN (1) throughout.
  readyState: number = 1; // WebSocket.OPEN
  bufferedAmount = 0;

  onopen: ((e: Event) => void) | null = null;
  onmessage: ((e: MessageEvent<ArrayBuffer>) => void) | null = null;
  onerror: ((e: Event) => void) | null = null;
  onclose: ((e: CloseEvent) => void) | null = null;

  /** Frames the transport sent to the "server". */
  readonly sent: Uint8Array[] = [];

  send(data: ArrayBuffer | Uint8Array | string): void {
    const bytes =
      data instanceof Uint8Array
        ? data
        : typeof data === "string"
          ? new TextEncoder().encode(data)
          : new Uint8Array(data);
    this.sent.push(bytes);
  }

  close(): void {
    this.onclose?.({} as CloseEvent);
  }

  /** Simulate the connection becoming open. */
  open(): void {
    this.onopen?.({} as Event);
  }

  /** Simulate the server pushing a raw frame to the client. */
  serverSend(frame: Uint8Array): void {
    const ab = frame.buffer.slice(
      frame.byteOffset,
      frame.byteOffset + frame.byteLength,
    );
    this.onmessage?.({ data: ab } as MessageEvent<ArrayBuffer>);
  }
}

function makeTransport(): { transport: WsGrpcTransport; ws: FakeWebSocket } {
  const ws = new FakeWebSocket();
  const transport = new WsGrpcTransport("ws://fake", {
    _wsFactory: () => ws as unknown as WebSocket,
  });
  ws.open(); // simulate immediate open
  return { transport, ws };
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function encodedFrame(
  type: number,
  streamId: number,
  payload: Uint8Array,
): Uint8Array {
  return encodeFrame({
    type: type as (typeof FrameType)[keyof typeof FrameType],
    streamId,
    payload,
  });
}

function serverEnd(streamId: number, statusCode = 0): Uint8Array {
  const ep = create(EndPayloadSchema, { statusCode, statusMessage: "" });
  return encodedFrame(FrameType.END, streamId, toBinary(EndPayloadSchema, ep));
}

function serverMsg(streamId: number, payload: Uint8Array): Uint8Array {
  return encodedFrame(FrameType.MSG, streamId, payload);
}

function serverHeader(streamId: number): Uint8Array {
  return encodedFrame(FrameType.HEADER, streamId, new Uint8Array(0));
}

// ── Transport state ───────────────────────────────────────────────────────────
describe("WsGrpcTransport state", () => {
  it("transitions to open when WebSocket opens", () => {
    const { transport } = makeTransport();
    expect(transport.state).toBe("open");
  });

  it("transitions to closed on WebSocket close", () => {
    const { transport, ws } = makeTransport();
    ws.close();
    expect(transport.state).toBe("closed");
  });
});

// ── Unary RPC ─────────────────────────────────────────────────────────────────
describe("unary RPC", () => {
  it("sends BEGIN+MSG+END then resolves with decoded response", async () => {
    const { transport, ws } = makeTransport();
    const reqBytes = new TextEncoder().encode("world");
    const respBytes = new TextEncoder().encode("hello world");

    const call = transport
      .openStream("/echo.v1.Echo/Echo")
      .unary(reqBytes, (b) => new TextDecoder().decode(b));

    // Transport must have sent BEGIN, MSG, END.
    expect(ws.sent).toHaveLength(3);
    expect(ws.sent[0]![9 - 9]); // frame type at byte 0
    expect(ws.sent[0]![0]).toBe(FrameType.BEGIN);
    expect(ws.sent[1]![0]).toBe(FrameType.MSG);
    expect(ws.sent[2]![0]).toBe(FrameType.END);

    // Verify BEGIN payload contains the method path.
    const bp = fromBinary(BeginPayloadSchema, ws.sent[0]!.slice(9));
    expect(bp.method).toBe("/echo.v1.Echo/Echo");

    // Server responds.
    ws.serverSend(serverHeader(1));
    ws.serverSend(serverMsg(1, respBytes));
    ws.serverSend(serverEnd(1, 0));

    expect(await call).toBe("hello world");
  });

  it("rejects when server returns non-OK status", async () => {
    const { transport, ws } = makeTransport();
    const call = transport
      .openStream("/echo.v1.Echo/Echo")
      .unary(new Uint8Array(0), (b) => b);

    const ep = create(EndPayloadSchema, {
      statusCode: 5,
      statusMessage: "not found",
    });
    ws.serverSend(
      encodedFrame(FrameType.END, 1, toBinary(EndPayloadSchema, ep)),
    );

    await expect(Promise.resolve(call)).rejects.toBeInstanceOf(GrpcStatusError);
  });
});

// ── Server-streaming RPC ──────────────────────────────────────────────────────
describe("server-streaming RPC", () => {
  it("receives multiple messages then terminates", async () => {
    const { transport, ws } = makeTransport();

    const ss = transport
      .openStream("/echo.v1.Echo/EchoStream")
      .serverStream(new TextEncoder().encode("ping"), (b) =>
        new TextDecoder().decode(b),
      );

    ws.serverSend(serverHeader(1));
    for (let i = 0; i < 3; i++) {
      ws.serverSend(serverMsg(1, new TextEncoder().encode("ping")));
    }
    ws.serverSend(serverEnd(1));

    const msgs: string[] = [];
    for await (const m of ss) {
      msgs.push(m);
    }
    expect(msgs).toEqual(["ping", "ping", "ping"]);
  });
});

// ── Client-streaming RPC ──────────────────────────────────────────────────────
describe("client-streaming RPC", () => {
  it("sends multiple messages then resolves with single response", async () => {
    const { transport, ws } = makeTransport();

    const cs = transport
      .openStream("/echo.v1.Echo/EchoCollect")
      .clientStream(
        (s: string) => new TextEncoder().encode(s),
        (b) => new TextDecoder().decode(b),
      );

    cs.send("a");
    cs.send("b");
    cs.send("c");

    const resultP = cs.closeAndReceive();

    // BEGIN + 3 MSG + END
    expect(ws.sent).toHaveLength(5);

    ws.serverSend(serverMsg(1, new TextEncoder().encode("a,b,c")));
    ws.serverSend(serverEnd(1));

    expect(await resultP).toBe("a,b,c");
  });
});

// ── Bidi-streaming RPC ────────────────────────────────────────────────────────
describe("bidi-streaming RPC", () => {
  it("full echo cycle: sends N messages, receives N echoes", async () => {
    const { transport, ws } = makeTransport();

    const bidi = transport
      .openStream("/echo.v1.Echo/EchoBidi")
      .bidiStream<string, string>(
        (s) => new TextEncoder().encode(s),
        (b) => new TextDecoder().decode(b),
      );

    const words = ["alpha", "beta", "gamma"];
    for (const w of words) {
      bidi.send(w);
    }

    // Server echoes each message then closes.
    ws.serverSend(serverHeader(1));
    for (const w of words) {
      ws.serverSend(serverMsg(1, new TextEncoder().encode(w)));
    }
    ws.serverSend(serverEnd(1));

    const received: string[] = [];
    for await (const m of bidi) {
      received.push(m);
    }
    expect(received).toEqual(words);
  });

  it("cancel() sends RESET and terminates the iterator", async () => {
    const { transport, ws } = makeTransport();

    const bidi = transport
      .openStream("/echo.v1.Echo/EchoBidi")
      .bidiStream<string, string>(
        (s) => new TextEncoder().encode(s),
        (b) => new TextDecoder().decode(b),
      );

    const iter = bidi[Symbol.asyncIterator]();
    const pendingNext = iter.next(); // suspend

    bidi.cancel();

    expect(ws.sent.some((f) => f[0] === FrameType.RESET)).toBe(true);
    const r = await pendingNext;
    expect(r.done).toBe(true);
  });
});

// ── Stream ID assignment ──────────────────────────────────────────────────────
describe("stream ID assignment", () => {
  it("assigns incrementing IDs starting at 1", () => {
    const { transport, ws } = makeTransport();

    transport.openStream("/svc/M1");
    transport.openStream("/svc/M2");
    transport.openStream("/svc/M3");

    const beginFrames = ws.sent.filter((f) => f[0] === FrameType.BEGIN);
    expect(beginFrames).toHaveLength(3);

    // Stream ID is bytes 1-4 big-endian.
    const ids = beginFrames.map((f) => (f[1] << 24) | (f[2] << 16) | (f[3] << 8) | f[4]);
    expect(ids).toEqual([1, 2, 3]);
  });
});

// ── WebSocket close resets all streams ────────────────────────────────────────
describe("WebSocket close", () => {
  it("resets all in-flight streams when connection closes", async () => {
    const { transport, ws } = makeTransport();

    const iter = transport
      .openStream("/svc/M")
      .bidiStream<string, string>(
        (s) => new TextEncoder().encode(s),
        (b) => new TextDecoder().decode(b),
      )[Symbol.asyncIterator]();

    const pendingNext = iter.next(); // suspended

    ws.close(); // simulate server disconnect

    const r = await pendingNext;
    expect(r.done).toBe(true);
    expect(transport.state).toBe("closed");
  });
});

