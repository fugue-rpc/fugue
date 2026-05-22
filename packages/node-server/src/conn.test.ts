// Week 2: FugueConn integration tests.
//
// Verifies the connection layer against a real WebSocket over loopback:
//   • All four RPC kinds (unary, server-stream, client-stream, bidi)
//   • Unimplemented method → END(UNIMPLEMENTED)
//   • Protocol errors (stream_id=0, non-monotonic ID) → WS close 1002
//   • RESET for unknown stream silently dropped
//   • RESET cancels an active stream
//   • 20 concurrent streams (the Week 2 done criterion)

import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { WebSocket, WebSocketServer } from "ws";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  ServiceRegistry,
  type ServiceDefinition,
  type UnaryHandler,
  type ServerStreamHandler,
  type ClientStreamHandler,
  type BidiHandler,
} from "./service.js";
import { FugueConn, type CallInterceptor } from "./conn.js";
import { decodeAll, encodeFrame, FrameType, type Frame } from "./frame.js";
import {
  BeginPayloadSchema, EndPayloadSchema,
} from "./proto.js";

// ----- Test codec -----
// JSON codec so tests don't need real proto messages.
type Msg = { value: string };
const serialize = (v: Msg): Buffer => Buffer.from(JSON.stringify(v));
const deserialize = (b: Buffer): Msg => JSON.parse(b.toString()) as Msg;

const EchoService = {
  echo: {
    path: "/echo/Echo",
    requestStream: false, responseStream: false,
    requestDeserialize: deserialize, responseSerialize: serialize,
  },
  echoStream: {
    path: "/echo/EchoStream",
    requestStream: false, responseStream: true,
    requestDeserialize: deserialize, responseSerialize: serialize,
  },
  echoCollect: {
    path: "/echo/EchoCollect",
    requestStream: true, responseStream: false,
    requestDeserialize: deserialize, responseSerialize: serialize,
  },
  echoBidi: {
    path: "/echo/EchoBidi",
    requestStream: true, responseStream: true,
    requestDeserialize: deserialize, responseSerialize: serialize,
  },
} satisfies ServiceDefinition;

// ----- Test server helpers -----

interface TestServer {
  url: string;
  close: () => Promise<void>;
}

function makeTestServer(registry: ServiceRegistry, opts?: import("./conn.js").ConnOptions): Promise<TestServer> {
  return new Promise((resolve) => {
    const httpServer = createServer();
    const wss = new WebSocketServer({ noServer: true });
    httpServer.on("upgrade", (req, socket, head) => {
      wss.handleUpgrade(req, socket, head, (ws) => {
        new FugueConn(ws, (p) => registry.lookup(p), opts).serve();
      });
    });
    httpServer.listen(0, "127.0.0.1", () => {
      const { port } = httpServer.address() as AddressInfo;
      resolve({
        url: `ws://127.0.0.1:${port}`,
        close: () => new Promise<void>((r) => httpServer.close(() => r())),
      });
    });
  });
}

// Frame demultiplexer: collects frames per stream ID.
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
    expect(f.type).toBe(type);
    return f;
  }
}

// Open a WebSocket and return the client + demux.
function connect(url: string): Promise<{ ws: WebSocket; demux: Demux; close: () => void }> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    ws.once("error", reject);
    ws.once("open", () => {
      ws.off("error", reject);
      const demux = new Demux(ws);
      resolve({ ws, demux, close: () => ws.close() });
    });
  });
}

// Encode and send a frame.
function sendFrame(ws: WebSocket, type: number, streamId: number, payload: Uint8Array = new Uint8Array(0)): void {
  ws.send(encodeFrame(type, streamId, payload));
}

function beginPayload(method: string, metadata: Record<string, string> = {}): Uint8Array {
  return toBinary(BeginPayloadSchema, create(BeginPayloadSchema, { method, metadata }));
}

function msgPayload(msg: Msg): Buffer {
  return serialize(msg);
}

function parseEndPayload(frame: Frame): { statusCode: number; statusMessage: string } {
  if (frame.payload.length === 0) return { statusCode: 0, statusMessage: "" };
  const ep = fromBinary(EndPayloadSchema, frame.payload);
  return { statusCode: ep.statusCode, statusMessage: ep.statusMessage };
}

// ----- Test suites -----

describe("FugueConn — RPC kinds", () => {
  let server: TestServer;
  let registry: ServiceRegistry;

  beforeEach(async () => {
    registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echo: (async (call) => ({ value: `echo:${call.request.value}` })) satisfies UnaryHandler<Msg, Msg>,
      echoStream: (async (call) => {
        for (let i = 0; i < 3; i++) call.write({ value: `${call.request.value}:${i}` });
      }) satisfies ServerStreamHandler<Msg, Msg>,
      echoCollect: (async (call) => {
        const parts: string[] = [];
        for await (const msg of call) parts.push(msg.value);
        return { value: parts.join(",") };
      }) satisfies ClientStreamHandler<Msg, Msg>,
      echoBidi: (async (call) => {
        for await (const msg of call) call.write({ value: `echo:${msg.value}` });
      }) satisfies BidiHandler<Msg, Msg>,
    });
    server = await makeTestServer(registry);
  });

  afterEach(async () => { await server.close(); });

  it("unary: BEGIN+MSG+END → HEADER+MSG+END", async () => {
    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/echo/Echo"));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "hello" }));
      sendFrame(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      const msgFrame = await demux.expectType(1, FrameType.MSG);
      const endFrame = await demux.expectType(1, FrameType.END);

      expect(deserialize(msgFrame.payload).value).toBe("echo:hello");
      expect(parseEndPayload(endFrame).statusCode).toBe(0);
    } finally { close(); }
  });

  it("server-streaming: handler writes 3 times", async () => {
    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/echo/EchoStream"));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "ping" }));
      sendFrame(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      for (let i = 0; i < 3; i++) {
        const f = await demux.expectType(1, FrameType.MSG);
        expect(deserialize(f.payload).value).toBe(`ping:${i}`);
      }
      await demux.expectType(1, FrameType.END);
    } finally { close(); }
  });

  it("client-streaming: collects all messages and returns one response", async () => {
    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/echo/EchoCollect"));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "a" }));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "b" }));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "c" }));
      sendFrame(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      const msgFrame = await demux.expectType(1, FrameType.MSG);
      await demux.expectType(1, FrameType.END);

      expect(deserialize(msgFrame.payload).value).toBe("a,b,c");
    } finally { close(); }
  });

  it("bidi-streaming: echoes each message as it arrives", async () => {
    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/echo/EchoBidi"));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "x" }));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "y" }));
      sendFrame(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      const m1 = await demux.expectType(1, FrameType.MSG);
      const m2 = await demux.expectType(1, FrameType.MSG);
      await demux.expectType(1, FrameType.END);

      expect(deserialize(m1.payload).value).toBe("echo:x");
      expect(deserialize(m2.payload).value).toBe("echo:y");
    } finally { close(); }
  });

  it("unimplemented method → END(UNIMPLEMENTED=12), no HEADER", async () => {
    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/no.Such/Method"));
      const endFrame = await demux.expectType(1, FrameType.END);
      expect(parseEndPayload(endFrame).statusCode).toBe(12); // UNIMPLEMENTED
    } finally { close(); }
  });

  it("metadata from BEGIN is visible to the handler", async () => {
    registry.addService(
      {
        check: {
          path: "/meta/Check",
          requestStream: false, responseStream: false,
          requestDeserialize: deserialize, responseSerialize: serialize,
        },
      },
      {
        check: async (call) => ({ value: call.metadata["x-token"] ?? "missing" }),
      },
    );

    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/meta/Check", { "x-token": "secret" }));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "" }));
      sendFrame(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      const f = await demux.expectType(1, FrameType.MSG);
      await demux.expectType(1, FrameType.END);
      expect(deserialize(f.payload).value).toBe("secret");
    } finally { close(); }
  });

  it("sendHeader / setTrailer are delivered", async () => {
    registry.addService(
      {
        tagged: {
          path: "/tagged/Call",
          requestStream: false, responseStream: false,
          requestDeserialize: deserialize, responseSerialize: serialize,
        },
      },
      {
        tagged: async (call) => {
          call.sendHeader({ "x-server": "node" });
          call.setTrailer({ "x-done": "1" });
          return { value: "ok" };
        },
      },
    );

    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/tagged/Call"));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "" }));
      sendFrame(ws, FrameType.END, 1);

      const headerFrame = await demux.expectType(1, FrameType.HEADER);
      await demux.expectType(1, FrameType.MSG);
      const endFrame = await demux.expectType(1, FrameType.END);

      // HEADER payload carries x-server
      const { create: c, fromBinary: fb } = await import("@bufbuild/protobuf");
      const { HeaderPayloadSchema: hs } = await import("./proto.js");
      const hp = fb(hs, headerFrame.payload);
      expect(hp.headers["x-server"]).toBe("node");

      // END payload carries trailers
      const ep = fromBinary(EndPayloadSchema, endFrame.payload);
      expect(ep.trailers["x-done"]).toBe("1");
    } finally { close(); }
  });
});

describe("FugueConn — protocol errors", () => {
  let server: TestServer;

  beforeEach(async () => {
    server = await makeTestServer(new ServiceRegistry());
  });

  afterEach(async () => { await server.close(); });

  it("stream_id = 0 causes WS close 1002", async () => {
    const { ws } = await connect(server.url);
    sendFrame(ws, FrameType.BEGIN, 0);
    await new Promise<void>((resolve) => {
      ws.on("close", (code) => {
        expect(code).toBe(1002);
        resolve();
      });
    });
  });

  it("non-monotonic stream_id causes WS close 1002", async () => {
    const { ws } = await connect(server.url);
    sendFrame(ws, FrameType.BEGIN, 5, beginPayload("/no/Op")); // accepted
    sendFrame(ws, FrameType.BEGIN, 3, beginPayload("/no/Op")); // non-monotonic
    await new Promise<void>((resolve) => {
      ws.on("close", (code) => {
        expect(code).toBe(1002);
        resolve();
      });
    });
  });

  it("text WebSocket frame causes WS close 1002", async () => {
    const ws = new WebSocket(server.url);
    await new Promise<void>((r) => ws.once("open", r));
    ws.send("not binary");
    await new Promise<void>((resolve) => {
      ws.on("close", (code) => {
        expect(code).toBe(1002);
        resolve();
      });
    });
  });
});

describe("FugueConn — RESET handling", () => {
  let server: TestServer;

  beforeEach(async () => {
    server = await makeTestServer(new ServiceRegistry());
  });

  afterEach(async () => { await server.close(); });

  it("RESET for unknown/closed stream is silently dropped (connection stays alive)", async () => {
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echo: async (call) => ({ value: `echo:${call.request.value}` }),
    });
    const srv = await makeTestServer(registry);
    const { ws, demux, close } = await connect(srv.url);
    try {
      // RESET for a stream that was never opened
      sendFrame(ws, FrameType.RESET, 99);

      // Follow-up valid stream must succeed, proving connection is alive
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/echo/Echo"));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "alive" }));
      sendFrame(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      const f = await demux.expectType(1, FrameType.MSG);
      await demux.expectType(1, FrameType.END);
      expect(deserialize(f.payload).value).toBe("echo:alive");
    } finally {
      close();
      await srv.close();
    }
  });

  it("RESET for active stream wakes up a blocked handler", async () => {
    let handlerStarted: () => void;
    let handlerDone: () => void;
    const startedP = new Promise<void>((r) => { handlerStarted = r; });
    const doneP = new Promise<void>((r) => { handlerDone = r; });

    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echoCollect: async (call) => {
        handlerStarted!();
        for await (const _msg of call) { /* drain — blocks until RESET or END */ }
        handlerDone!();
        return { value: "" };
      },
    });

    const srv = await makeTestServer(registry);
    const { ws, demux, close } = await connect(srv.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/echo/EchoCollect"));
      await startedP;

      sendFrame(ws, FrameType.RESET, 1);
      await doneP; // handler must exit after RESET
      // END with INTERNAL because handler exits after being reset
      // (we just verify the handler exited cleanly)
    } finally {
      close();
      await srv.close();
    }
  });
});

describe("FugueConn — 20 concurrent streams (Week 2 done criterion)", () => {
  it("20 concurrent unary streams complete without race conditions", async () => {
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echo: async (call) => ({ value: `echo:${call.request.value}` }),
    });
    const server = await makeTestServer(registry);
    const { ws, demux, close } = await connect(server.url);

    try {
      const N = 20;
      // Send all BEGINs in strict ascending order (monotonicity requirement).
      for (let i = 1; i <= N; i++) {
        sendFrame(ws, FrameType.BEGIN, i, beginPayload("/echo/Echo"));
      }

      // Send MSG+END for all streams (order doesn't matter after BEGIN).
      for (let i = 1; i <= N; i++) {
        sendFrame(ws, FrameType.MSG, i, msgPayload({ value: `stream-${i}` }));
        sendFrame(ws, FrameType.END, i);
      }

      // Collect HEADER+MSG+END for all streams concurrently.
      const results = await Promise.all(
        Array.from({ length: N }, async (_, idx) => {
          const id = idx + 1;
          await demux.expectType(id, FrameType.HEADER);
          const f = await demux.expectType(id, FrameType.MSG);
          await demux.expectType(id, FrameType.END);
          return deserialize(f.payload).value;
        }),
      );

      for (let i = 0; i < N; i++) {
        expect(results[i]).toBe(`echo:stream-${i + 1}`);
      }
    } finally {
      close();
      await server.close();
    }
  });
});

describe("FugueConn — interceptors", () => {
  it("interceptor that throws sends its gRPC status before the handler runs", async () => {
    let handlerCalled = false;
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echo: async () => { handlerCalled = true; return { value: "should not reach" }; },
    });
    const interceptor: CallInterceptor = async () => {
      throw Object.assign(new Error("UNAUTHENTICATED"), { code: 16 });
    };
    const server = await makeTestServer(registry, { interceptor });
    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/echo/Echo"));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "x" }));
      sendFrame(ws, FrameType.END, 1);

      const end = await demux.expectType(1, FrameType.END);
      expect(parseEndPayload(end).statusCode).toBe(16); // UNAUTHENTICATED
      expect(handlerCalled).toBe(false);
    } finally {
      close();
      await server.close();
    }
  });

  it("interceptor that returns normally allows the handler to run", async () => {
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echo: async (call) => ({ value: `echo:${call.request.value}` }),
    });
    let interceptorCalled = false;
    const interceptor: CallInterceptor = async () => { interceptorCalled = true; };
    const server = await makeTestServer(registry, { interceptor });
    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/echo/Echo"));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "hi" }));
      sendFrame(ws, FrameType.END, 1);

      await demux.expectType(1, FrameType.HEADER);
      const f = await demux.expectType(1, FrameType.MSG);
      await demux.expectType(1, FrameType.END);

      expect(deserialize(f.payload).value).toBe("echo:hi");
      expect(interceptorCalled).toBe(true);
    } finally {
      close();
      await server.close();
    }
  });

  it("interceptor receives the correct method path and metadata", async () => {
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echo: async (call) => ({ value: call.request.value }),
    });
    let capturedCtx: { method: string; metadata: Record<string, string> } | undefined;
    const interceptor: CallInterceptor = async (ctx) => { capturedCtx = ctx; };
    const server = await makeTestServer(registry, { interceptor });
    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/echo/Echo", { authorization: "Bearer tok" }));
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "" }));
      sendFrame(ws, FrameType.END, 1);
      await demux.expectType(1, FrameType.HEADER);
      await demux.expectType(1, FrameType.MSG);
      await demux.expectType(1, FrameType.END);

      expect(capturedCtx?.method).toBe("/echo/Echo");
      expect(capturedCtx?.metadata["authorization"]).toBe("Bearer tok");
    } finally {
      close();
      await server.close();
    }
  });

  it("interceptor rejection takes priority over UNIMPLEMENTED for unknown methods", async () => {
    const interceptor: CallInterceptor = async () => {
      throw Object.assign(new Error("UNAUTHENTICATED"), { code: 16 });
    };
    const server = await makeTestServer(new ServiceRegistry(), { interceptor });
    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/no/Such/Method"));
      const end = await demux.expectType(1, FrameType.END);
      // Should be UNAUTHENTICATED (16), not UNIMPLEMENTED (12)
      expect(parseEndPayload(end).statusCode).toBe(16);
    } finally {
      close();
      await server.close();
    }
  });

  it("interceptor without code throws → INTERNAL (13)", async () => {
    const interceptor: CallInterceptor = async () => { throw new Error("oops"); };
    const server = await makeTestServer(new ServiceRegistry(), { interceptor });
    const { ws, demux, close } = await connect(server.url);
    try {
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/any/Method"));
      const end = await demux.expectType(1, FrameType.END);
      expect(parseEndPayload(end).statusCode).toBe(13); // INTERNAL
    } finally {
      close();
      await server.close();
    }
  });
});

describe("FugueConn — maxStreams", () => {
  it("(N+1)th stream receives RESOURCE_EXHAUSTED, connection stays alive", async () => {
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      // Bidi handler that blocks until the client sends END
      echoBidi: async (call) => {
        for await (const msg of call) call.write({ value: `echo:${msg.value}` });
      },
    });

    const server = await makeTestServer(registry, { maxStreams: 2 });
    const { ws, demux, close } = await connect(server.url);

    try {
      // Open 2 streams
      sendFrame(ws, FrameType.BEGIN, 1, beginPayload("/echo/EchoBidi"));
      sendFrame(ws, FrameType.BEGIN, 2, beginPayload("/echo/EchoBidi"));

      // Ping each to confirm they're running
      sendFrame(ws, FrameType.MSG, 1, msgPayload({ value: "ping" }));
      sendFrame(ws, FrameType.MSG, 2, msgPayload({ value: "ping" }));

      await demux.expectType(1, FrameType.HEADER);
      await demux.expectType(1, FrameType.MSG);
      await demux.expectType(2, FrameType.HEADER);
      await demux.expectType(2, FrameType.MSG);

      // Third stream must be rejected
      sendFrame(ws, FrameType.BEGIN, 3, beginPayload("/echo/EchoBidi"));
      const endFrame = await demux.expectType(3, FrameType.END);
      expect(parseEndPayload(endFrame).statusCode).toBe(8); // RESOURCE_EXHAUSTED

      // Close the open streams; they must complete normally
      sendFrame(ws, FrameType.END, 1);
      sendFrame(ws, FrameType.END, 2);
      await demux.expectType(1, FrameType.END);
      await demux.expectType(2, FrameType.END);
    } finally {
      close();
      await server.close();
    }
  });
});
