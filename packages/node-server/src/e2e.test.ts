// Week 3: FugueServer end-to-end tests.
//
// Spins up a FugueServer backed by all four Echo RPC kinds and drives it
// with real proto-encoded echo.v1.Msg messages over a loopback WebSocket.
// Covers the full path: HTTP upgrade → FugueServer → FugueConn → handler.
//
// Also covers origin-enforcement (the HTTP layer that conn.test.ts cannot test).
//
// Proto codec — echo.v1.Msg { string value = 1 }:
//   field tag 0x0a (field 1, wire type 2) + 1-byte varint length + UTF-8 value.
//   Matches the hand-rolled codec used by packages/transport/src/e2e.test.ts.

import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { WebSocket } from "ws";
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import {
  FugueServer,
  type ServiceDefinition,
  type UnaryHandler,
  type ServerStreamHandler,
  type ClientStreamHandler,
  type BidiHandler,
} from "./index.js";
import { decodeAll, encodeFrame, FrameType, type Frame } from "./frame.js";
import { BeginPayloadSchema, EndPayloadSchema } from "./proto.js";
import { fromBinary } from "@bufbuild/protobuf";

// ----- Proto codec for echo.v1.Msg { string value = 1 } -----

function encodeMsg(value: string): Buffer {
  const data = Buffer.from(value, "utf8");
  // Field 1, wire type 2 (length-delimited): tag=0x0a, single-byte varint length.
  // Supports message values up to 127 bytes, which is enough for all test strings.
  const out = Buffer.allocUnsafe(2 + data.length);
  out[0] = 0x0a;
  out[1] = data.length;
  data.copy(out, 2);
  return out;
}

function decodeMsg(buf: Buffer): string {
  return buf.subarray(2).toString("utf8");
}

// ----- Service definition -----

const EchoService = {
  echo:         { path: "/echo.v1.Echo/Echo",        requestStream: false, responseStream: false, requestDeserialize: decodeMsg, responseSerialize: encodeMsg },
  echoStream:   { path: "/echo.v1.Echo/EchoStream",  requestStream: false, responseStream: true,  requestDeserialize: decodeMsg, responseSerialize: encodeMsg },
  echoCollect:  { path: "/echo.v1.Echo/EchoCollect", requestStream: true,  responseStream: false, requestDeserialize: decodeMsg, responseSerialize: encodeMsg },
  echoBidi:     { path: "/echo.v1.Echo/EchoBidi",    requestStream: true,  responseStream: true,  requestDeserialize: decodeMsg, responseSerialize: encodeMsg },
} satisfies ServiceDefinition;

// ----- Server factory -----

interface TestServer {
  url: string;
  close(): Promise<void>;
}

function startEchoServer(opts?: ConstructorParameters<typeof FugueServer>[0]): Promise<TestServer> {
  const fugueServer = new FugueServer(opts);
  fugueServer.addService(EchoService, {
    echo: (async (call) => call.request) satisfies UnaryHandler<string, string>,

    echoStream: (async (call) => {
      for (let i = 0; i < 5; i++) call.write(call.request);
    }) satisfies ServerStreamHandler<string, string>,

    echoCollect: (async (call) => {
      const parts: string[] = [];
      for await (const v of call) parts.push(v);
      return parts.join(",");
    }) satisfies ClientStreamHandler<string, string>,

    echoBidi: (async (call) => {
      for await (const v of call) call.write(v);
    }) satisfies BidiHandler<string, string>,
  });

  const httpServer = createServer();
  fugueServer.attach(httpServer, "/fugue/");
  return new Promise((resolve) => {
    httpServer.listen(0, "127.0.0.1", () => {
      const { port } = httpServer.address() as AddressInfo;
      resolve({
        url: `ws://127.0.0.1:${port}/fugue/`,
        close: () => new Promise<void>((r) => httpServer.close(() => r())),
      });
    });
  });
}

// ----- WebSocket test helpers (same as conn.test.ts) -----

class Demux {
  private readonly _queues = new Map<number, Frame[]>();
  private readonly _waiters = new Map<number, () => void>();

  constructor(ws: WebSocket) {
    ws.on("message", (data) => {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer);
      for (const frame of decodeAll(buf)) {
        const q = this._queues.get(frame.streamId) ?? [];
        q.push(frame);
        this._queues.set(frame.streamId, q);
        const w = this._waiters.get(frame.streamId);
        if (w) { this._waiters.delete(frame.streamId); w(); }
      }
    });
  }

  next(streamId: number): Promise<Frame> {
    const q = this._queues.get(streamId);
    if (q && q.length > 0) {
      const f = q.shift()!;
      if (q.length === 0) this._queues.delete(streamId);
      return Promise.resolve(f);
    }
    return new Promise<Frame>((resolve) => {
      this._waiters.set(streamId, () => {
        const q2 = this._queues.get(streamId)!;
        const f = q2.shift()!;
        if (q2.length === 0) this._queues.delete(streamId);
        resolve(f);
      });
    });
  }

  async expectType(streamId: number, type: number): Promise<Frame> {
    const f = await this.next(streamId);
    expect(f.type, `stream ${streamId}: expected frame type 0x${type.toString(16)}, got 0x${f.type.toString(16)}`).toBe(type);
    return f;
  }
}

function connect(url: string, wsOpts?: ConstructorParameters<typeof WebSocket>[1]): Promise<{ ws: WebSocket; demux: Demux; close: () => void }> {
  return new Promise((resolve, reject) => {
    // Cast needed: ws library accepts options as second arg, types vary by overload.
    const ws = new WebSocket(url, wsOpts as string | undefined);
    ws.once("error", reject);
    ws.once("open", () => {
      ws.off("error", reject);
      resolve({ ws, demux: new Demux(ws), close: () => ws.close() });
    });
  });
}

function send(ws: WebSocket, type: number, streamId: number, payload: Uint8Array = new Uint8Array(0)): void {
  ws.send(encodeFrame(type, streamId, payload));
}

function beginPayload(method: string, metadata: Record<string, string> = {}): Uint8Array {
  return toBinary(BeginPayloadSchema, create(BeginPayloadSchema, { method, metadata }));
}

function parseEndStatus(frame: Frame): number {
  if (frame.payload.length === 0) return 0;
  return fromBinary(EndPayloadSchema, frame.payload).statusCode;
}

// ----- Tests -----

describe("FugueServer — four RPC kinds with proto-encoded messages", () => {
  let server: TestServer;

  beforeAll(async () => {
    server = await startEchoServer({ origins: "*" });
  });

  afterAll(async () => {
    await server.close();
  });

  it("unary: echoes a single message", async () => {
    const { ws, demux, close } = await connect(server.url);
    try {
      send(ws, FrameType.BEGIN, 1, beginPayload("/echo.v1.Echo/Echo"));
      send(ws, FrameType.MSG, 1, encodeMsg("hello fugue"));
      send(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      const msg = await demux.expectType(1, FrameType.MSG);
      const end = await demux.expectType(1, FrameType.END);

      expect(decodeMsg(msg.payload)).toBe("hello fugue");
      expect(parseEndStatus(end)).toBe(0); // OK
    } finally { close(); }
  });

  it("unary: unicode payload round-trips correctly", async () => {
    const { ws, demux, close } = await connect(server.url);
    try {
      send(ws, FrameType.BEGIN, 1, beginPayload("/echo.v1.Echo/Echo"));
      send(ws, FrameType.MSG, 1, encodeMsg("こんにちは 🌍"));
      send(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      const msg = await demux.expectType(1, FrameType.MSG);
      await demux.expectType(1, FrameType.END);

      expect(decodeMsg(msg.payload)).toBe("こんにちは 🌍");
    } finally { close(); }
  });

  it("server-streaming: receives exactly 5 echoes", async () => {
    const { ws, demux, close } = await connect(server.url);
    try {
      send(ws, FrameType.BEGIN, 1, beginPayload("/echo.v1.Echo/EchoStream"));
      send(ws, FrameType.MSG, 1, encodeMsg("ping"));
      send(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      const msgs: string[] = [];
      for (let i = 0; i < 5; i++) {
        msgs.push(decodeMsg((await demux.expectType(1, FrameType.MSG)).payload));
      }
      await demux.expectType(1, FrameType.END);

      expect(msgs).toEqual(["ping", "ping", "ping", "ping", "ping"]);
    } finally { close(); }
  });

  it("client-streaming: collected values joined with commas", async () => {
    const { ws, demux, close } = await connect(server.url);
    try {
      send(ws, FrameType.BEGIN, 1, beginPayload("/echo.v1.Echo/EchoCollect"));
      send(ws, FrameType.MSG, 1, encodeMsg("alpha"));
      send(ws, FrameType.MSG, 1, encodeMsg("beta"));
      send(ws, FrameType.MSG, 1, encodeMsg("gamma"));
      send(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      const msg = await demux.expectType(1, FrameType.MSG);
      await demux.expectType(1, FrameType.END);

      expect(decodeMsg(msg.payload)).toBe("alpha,beta,gamma");
    } finally { close(); }
  });

  it("bidi-streaming: each sent message is echoed in order", async () => {
    const { ws, demux, close } = await connect(server.url);
    try {
      send(ws, FrameType.BEGIN, 1, beginPayload("/echo.v1.Echo/EchoBidi"));
      send(ws, FrameType.MSG, 1, encodeMsg("x"));
      send(ws, FrameType.MSG, 1, encodeMsg("y"));
      send(ws, FrameType.MSG, 1, encodeMsg("z"));
      send(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      const echoes = [
        decodeMsg((await demux.expectType(1, FrameType.MSG)).payload),
        decodeMsg((await demux.expectType(1, FrameType.MSG)).payload),
        decodeMsg((await demux.expectType(1, FrameType.MSG)).payload),
      ];
      await demux.expectType(1, FrameType.END);

      expect(echoes).toEqual(["x", "y", "z"]);
    } finally { close(); }
  });

  it("multiple concurrent streams on one connection", async () => {
    const { ws, demux, close } = await connect(server.url);
    const N = 10;
    try {
      for (let i = 1; i <= N; i++) {
        send(ws, FrameType.BEGIN, i, beginPayload("/echo.v1.Echo/Echo"));
      }
      for (let i = 1; i <= N; i++) {
        send(ws, FrameType.MSG, i, encodeMsg(`item-${i}`));
        send(ws, FrameType.END, i);
      }

      const results = await Promise.all(
        Array.from({ length: N }, async (_, idx) => {
          const id = idx + 1;
          await demux.expectType(id, FrameType.HEADER);
          const f = await demux.expectType(id, FrameType.MSG);
          await demux.expectType(id, FrameType.END);
          return decodeMsg(f.payload);
        }),
      );

      for (let i = 0; i < N; i++) {
        expect(results[i]).toBe(`item-${i + 1}`);
      }
    } finally { close(); }
  });

  it("handler errors produce END with INTERNAL status (13)", async () => {
    const srv = new FugueServer({ origins: "*" });
    srv.addService(
      {
        fail: {
          path: "/test/Fail",
          requestStream: false, responseStream: false,
          requestDeserialize: decodeMsg, responseSerialize: encodeMsg,
        },
      },
      {
        fail: async () => { throw new Error("handler exploded"); },
      },
    );
    const httpServer = createServer();
    srv.attach(httpServer, "/");
    await new Promise<void>((r) => httpServer.listen(0, "127.0.0.1", r));
    const { port } = httpServer.address() as AddressInfo;
    const { ws, demux, close } = await connect(`ws://127.0.0.1:${port}/`);
    try {
      send(ws, FrameType.BEGIN, 1, beginPayload("/test/Fail"));
      send(ws, FrameType.MSG, 1, encodeMsg("trigger"));
      send(ws, FrameType.END, 1);

      const end = await demux.expectType(1, FrameType.END);
      expect(parseEndStatus(end)).toBe(13); // INTERNAL
    } finally {
      close();
      await new Promise<void>((r) => httpServer.close(() => r()));
    }
  });

  it("custom gRPC error propagates status code", async () => {
    const srv = new FugueServer({ origins: "*" });
    srv.addService(
      {
        notFound: {
          path: "/test/NotFound",
          requestStream: false, responseStream: false,
          requestDeserialize: decodeMsg, responseSerialize: encodeMsg,
        },
      },
      {
        notFound: async () => {
          throw Object.assign(new Error("not found"), { code: 5 }); // NOT_FOUND
        },
      },
    );
    const httpServer = createServer();
    srv.attach(httpServer, "/");
    await new Promise<void>((r) => httpServer.listen(0, "127.0.0.1", r));
    const { port } = httpServer.address() as AddressInfo;
    const { ws, demux, close } = await connect(`ws://127.0.0.1:${port}/`);
    try {
      send(ws, FrameType.BEGIN, 1, beginPayload("/test/NotFound"));
      send(ws, FrameType.MSG, 1, encodeMsg("anything"));
      send(ws, FrameType.END, 1);

      const end = await demux.expectType(1, FrameType.END);
      expect(parseEndStatus(end)).toBe(5); // NOT_FOUND
    } finally {
      close();
      await new Promise<void>((r) => httpServer.close(() => r()));
    }
  });
});

describe("FugueServer — origin enforcement", () => {
  it("origins not configured: blocks browser Origin, allows non-browser (no Origin)", async () => {
    const srv = new FugueServer(); // default: block browser origins
    const httpServer = createServer();
    srv.attach(httpServer, "/");
    await new Promise<void>((r) => httpServer.listen(0, "127.0.0.1", r));
    const { port } = httpServer.address() as AddressInfo;
    const url = `ws://127.0.0.1:${port}/`;

    // Browser client (has Origin header) → rejected
    const rejected = await new Promise<boolean>((resolve) => {
      const ws = new WebSocket(url, { headers: { origin: "https://attacker.example.com" } } as never);
      ws.on("error", () => resolve(true));    // rejection = expected
      ws.on("open",  () => { ws.close(); resolve(false); }); // connection opened = unexpected
    });
    expect(rejected).toBe(true);

    // Non-browser client (no Origin header) → accepted
    const { ws, demux, close } = await connect(url);
    try {
      send(ws, FrameType.BEGIN, 1, beginPayload("/no/Op")); // no-op BEGIN
      // Server sends END(UNIMPLEMENTED) proving the WebSocket is open
      const end = await demux.expectType(1, FrameType.END);
      expect(parseEndStatus(end)).toBe(12); // UNIMPLEMENTED
    } finally { close(); }

    await new Promise<void>((r) => httpServer.close(() => r()));
  });

  it("origins: '*' allows all origins including browser", async () => {
    const srv = new FugueServer({ origins: "*" });
    const httpServer = createServer();
    srv.attach(httpServer, "/");
    await new Promise<void>((r) => httpServer.listen(0, "127.0.0.1", r));
    const { port } = httpServer.address() as AddressInfo;
    const url = `ws://127.0.0.1:${port}/`;

    const { ws, demux, close } = await connect(url, { headers: { origin: "https://anything.example.com" } } as never);
    try {
      send(ws, FrameType.BEGIN, 1, beginPayload("/no/Op"));
      const end = await demux.expectType(1, FrameType.END);
      expect(parseEndStatus(end)).toBe(12); // UNIMPLEMENTED — connection was accepted
    } finally { close(); }

    await new Promise<void>((r) => httpServer.close(() => r()));
  });

  it("specific allow-list: blocks non-matching origin, allows matching, allows no-origin", async () => {
    const srv = new FugueServer({ origins: "https://allowed.example.com" });
    const httpServer = createServer();
    srv.attach(httpServer, "/");
    await new Promise<void>((r) => httpServer.listen(0, "127.0.0.1", r));
    const { port } = httpServer.address() as AddressInfo;
    const url = `ws://127.0.0.1:${port}/`;

    // Blocked origin
    const blocked = await new Promise<boolean>((resolve) => {
      const ws = new WebSocket(url, { headers: { origin: "https://evil.com" } } as never);
      ws.on("error", () => resolve(true));
      ws.on("open",  () => { ws.close(); resolve(false); });
    });
    expect(blocked).toBe(true);

    // Allowed origin
    const { ws: ws2, demux: d2, close: c2 } = await connect(url, { headers: { origin: "https://allowed.example.com" } } as never);
    try {
      send(ws2, FrameType.BEGIN, 1, beginPayload("/no/Op"));
      await d2.expectType(1, FrameType.END); // UNIMPLEMENTED — accepted
    } finally { c2(); }

    // No-origin (non-browser) — always allowed
    const { ws: ws3, demux: d3, close: c3 } = await connect(url);
    try {
      send(ws3, FrameType.BEGIN, 1, beginPayload("/no/Op"));
      await d3.expectType(1, FrameType.END); // accepted
    } finally { c3(); }

    await new Promise<void>((r) => httpServer.close(() => r()));
  });

  it("path filtering: requests to a different path are rejected immediately", async () => {
    const srv = new FugueServer({ origins: "*" });
    const httpServer = createServer();
    srv.attach(httpServer, "/fugue/");
    await new Promise<void>((r) => httpServer.listen(0, "127.0.0.1", r));
    const { port } = httpServer.address() as AddressInfo;

    // socket.destroy() in attach() causes an immediate error on the client side.
    const rejected = await new Promise<boolean>((resolve) => {
      const ws = new WebSocket(`ws://127.0.0.1:${port}/other/`);
      ws.on("error", () => resolve(true));
      ws.on("open",  () => { ws.close(); resolve(false); });
    });
    expect(rejected).toBe(true);

    await new Promise<void>((r) => httpServer.close(() => r()));
  });

  it("addService chaining: multiple services reachable on same server", async () => {
    const srv = new FugueServer({ origins: "*" });
    srv
      .addService(
        { greet: { path: "/a/Greet", requestStream: false, responseStream: false, requestDeserialize: decodeMsg, responseSerialize: encodeMsg } },
        { greet: async (call) => `hello:${call.request}` },
      )
      .addService(
        { goodbye: { path: "/b/Bye", requestStream: false, responseStream: false, requestDeserialize: decodeMsg, responseSerialize: encodeMsg } },
        { goodbye: async (call) => `bye:${call.request}` },
      );

    const httpServer = createServer();
    srv.attach(httpServer, "/");
    await new Promise<void>((r) => httpServer.listen(0, "127.0.0.1", r));
    const { port } = httpServer.address() as AddressInfo;
    const { ws, demux, close } = await connect(`ws://127.0.0.1:${port}/`);

    try {
      send(ws, FrameType.BEGIN, 1, beginPayload("/a/Greet"));
      send(ws, FrameType.MSG, 1, encodeMsg("world"));
      send(ws, FrameType.END, 1);

      send(ws, FrameType.BEGIN, 2, beginPayload("/b/Bye"));
      send(ws, FrameType.MSG, 2, encodeMsg("world"));
      send(ws, FrameType.END, 2);

      await demux.expectType(1, FrameType.HEADER);
      const m1 = await demux.expectType(1, FrameType.MSG);
      await demux.expectType(1, FrameType.END);

      await demux.expectType(2, FrameType.HEADER);
      const m2 = await demux.expectType(2, FrameType.MSG);
      await demux.expectType(2, FrameType.END);

      expect(decodeMsg(m1.payload)).toBe("hello:world");
      expect(decodeMsg(m2.payload)).toBe("bye:world");
    } finally {
      close();
      await new Promise<void>((r) => httpServer.close(() => r()));
    }
  });
});

describe("FugueServer — graceful shutdown", () => {
  it("close() with no active connections resolves immediately", async () => {
    const srv = new FugueServer({ origins: "*" });
    const httpServer = createServer();
    srv.attach(httpServer, "/");
    await new Promise<void>((r) => httpServer.listen(0, "127.0.0.1", r));

    await srv.close(); // must not hang

    await new Promise<void>((r) => httpServer.close(() => r()));
  });

  it("close() sends WebSocket 1001 and resolves once all connections close", async () => {
    const srv = new FugueServer({ origins: "*" });
    srv.addService(EchoService, {
      echoBidi: async (call) => { for await (const v of call) call.write(v); },
    });
    const httpServer = createServer();
    srv.attach(httpServer, "/");
    await new Promise<void>((r) => httpServer.listen(0, "127.0.0.1", r));
    const { port } = httpServer.address() as AddressInfo;

    const { ws } = await connect(`ws://127.0.0.1:${port}/`);

    // Start a long-lived bidi stream to keep the connection open.
    send(ws, FrameType.BEGIN, 1, beginPayload("/echo.v1.Echo/EchoBidi"));

    const closeCode = await new Promise<number>((resolve) => {
      ws.on("close", (code) => resolve(code));
      srv.close().then(() => httpServer.close());
    });

    expect(closeCode).toBe(1001);
  });

  it("after close(), new upgrade attempts are rejected", async () => {
    const srv = new FugueServer({ origins: "*" });
    const httpServer = createServer();
    srv.attach(httpServer, "/");
    await new Promise<void>((r) => httpServer.listen(0, "127.0.0.1", r));
    const { port } = httpServer.address() as AddressInfo;

    await srv.close();

    const rejected = await new Promise<boolean>((resolve) => {
      const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
      ws.on("error", () => resolve(true));
      ws.on("open",  () => { ws.close(); resolve(false); });
    });
    expect(rejected).toBe(true);

    await new Promise<void>((r) => httpServer.close(() => r()));
  });
});
