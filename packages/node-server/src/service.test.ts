// Spike A: ServiceDefinition dispatch.
//
// Proves that ServiceRegistry correctly:
//   1. Accepts a service definition shaped like @grpc/grpc-js protoc output
//   2. Identifies the RPC kind for each method
//   3. Wires deserialize/serialize from the definition through to the handler
//   4. Dispatches all four handler shapes (unary, server-stream, client-stream, bidi)
//
// No WebSocket code. This is the contract that conn.ts (Week 2) will depend on.

import { describe, it, expect, vi } from "vitest";
import {
  ServiceRegistry,
  type ServiceDefinition,
  type UnaryServerCall,
  type ServerStreamCall,
  type ClientStreamCall,
  type BidiCall,
  type UnaryHandler,
  type ServerStreamHandler,
  type ClientStreamHandler,
  type BidiHandler,
} from "./service.js";

// Minimal stand-in for proto message types.
// Using JSON so that serialize/deserialize are easy to reason about in tests.
type Req = { value: string };
type Res = { value: string };

const serialize = (v: Res): Buffer => Buffer.from(JSON.stringify(v));
const deserialize = (b: Buffer): Req => JSON.parse(b.toString()) as Req;

// A service definition shaped exactly like protoc-gen-grpc-js output.
const EchoService = {
  echo: {
    path: "/echo.v1.Echo/Echo",
    requestStream: false,
    responseStream: false,
    requestDeserialize: deserialize,
    responseSerialize: serialize,
  },
  echoStream: {
    path: "/echo.v1.Echo/EchoStream",
    requestStream: false,
    responseStream: true,
    requestDeserialize: deserialize,
    responseSerialize: serialize,
  },
  echoCollect: {
    path: "/echo.v1.Echo/EchoCollect",
    requestStream: true,
    responseStream: false,
    requestDeserialize: deserialize,
    responseSerialize: serialize,
  },
  echoBidi: {
    path: "/echo.v1.Echo/EchoBidi",
    requestStream: true,
    responseStream: true,
    requestDeserialize: deserialize,
    responseSerialize: serialize,
  },
} satisfies ServiceDefinition;

// No-op call base shared across tests that don't need to assert on headers.
const noopBase = {
  metadata: {},
  sendHeader: vi.fn(),
  setTrailer: vi.fn(),
};

// Build an async iterable from an array — simulates the per-stream recv channel.
async function* fromArray<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) yield item;
}

describe("ServiceRegistry — registration", () => {
  it("registers methods and identifies their RPC kind", () => {
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echo: async (call) => ({ value: call.request.value }),
      echoStream: async () => {},
      echoCollect: async () => ({ value: "" }),
      echoBidi: async () => {},
    });

    expect(registry.lookup("/echo.v1.Echo/Echo")?.kind).toBe("unary");
    expect(registry.lookup("/echo.v1.Echo/EchoStream")?.kind).toBe("server_stream");
    expect(registry.lookup("/echo.v1.Echo/EchoCollect")?.kind).toBe("client_stream");
    expect(registry.lookup("/echo.v1.Echo/EchoBidi")?.kind).toBe("bidi_stream");
  });

  it("returns undefined for unregistered paths", () => {
    const registry = new ServiceRegistry();
    registry.addService(EchoService, { echo: async (call) => ({ value: call.request.value }) });
    expect(registry.lookup("/echo.v1.Echo/NoSuchMethod")).toBeUndefined();
  });

  it("silently skips methods with no handler in the implementation", () => {
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echo: async (call) => ({ value: call.request.value }),
      // echoStream intentionally omitted
    });
    expect(registry.lookup("/echo.v1.Echo/Echo")).toBeDefined();
    expect(registry.lookup("/echo.v1.Echo/EchoStream")).toBeUndefined();
  });

  it("registeredPaths returns all registered method paths", () => {
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echo: async (call) => ({ value: call.request.value }),
      echoStream: async () => {},
    });
    expect(registry.registeredPaths().sort()).toEqual([
      "/echo.v1.Echo/Echo",
      "/echo.v1.Echo/EchoStream",
    ]);
  });

  it("allows addService to be called multiple times for different services", () => {
    const OtherService = {
      ping: {
        path: "/other.v1.Other/Ping",
        requestStream: false,
        responseStream: false,
        requestDeserialize: deserialize,
        responseSerialize: serialize,
      },
    } satisfies ServiceDefinition;

    const registry = new ServiceRegistry();
    registry.addService(EchoService, { echo: async (call) => ({ value: call.request.value }) });
    registry.addService(OtherService, { ping: async (call) => ({ value: call.request.value }) });

    expect(registry.lookup("/echo.v1.Echo/Echo")).toBeDefined();
    expect(registry.lookup("/other.v1.Other/Ping")).toBeDefined();
  });
});

describe("ServiceRegistry — unary dispatch", () => {
  it("deserializes the raw request buffer, calls handler, result serializes correctly", async () => {
    const registry = new ServiceRegistry();
    const handler: UnaryHandler<Req, Res> = async (call) => ({
      value: `echo:${call.request.value}`,
    });
    registry.addService(EchoService, { echo: handler });

    const entry = registry.lookup("/echo.v1.Echo/Echo")!;

    // Simulate conn.ts: deserialize raw bytes from the MSG frame, build call,
    // invoke handler, serialize the returned response.
    const rawReq = Buffer.from(JSON.stringify({ value: "hello" }));
    const req = entry.deserialize(rawReq) as Req;

    const call: UnaryServerCall<Req> = { ...noopBase, request: req };
    const result = await (entry.handler as UnaryHandler<Req, Res>)(call);

    expect(result.value).toBe("echo:hello");
    // Round-trip through serialize
    const rawRes = entry.serialize(result);
    expect(JSON.parse(rawRes.toString())).toEqual({ value: "echo:hello" });
  });

  it("exposes incoming metadata on the call object", async () => {
    const registry = new ServiceRegistry();
    let capturedMeta: Record<string, string> = {};
    registry.addService(EchoService, {
      echo: async (call) => {
        capturedMeta = call.metadata;
        return { value: "" };
      },
    });

    const entry = registry.lookup("/echo.v1.Echo/Echo")!;
    const call: UnaryServerCall<Req> = {
      ...noopBase,
      metadata: { authorization: "bearer token123", "x-request-id": "req-1" },
      request: { value: "" },
    };
    await (entry.handler as UnaryHandler<Req, Res>)(call);

    expect(capturedMeta["authorization"]).toBe("bearer token123");
    expect(capturedMeta["x-request-id"]).toBe("req-1");
  });

  it("handler can call sendHeader and setTrailer", async () => {
    const registry = new ServiceRegistry();
    const sendHeader = vi.fn();
    const setTrailer = vi.fn();
    registry.addService(EchoService, {
      echo: async (call) => {
        call.sendHeader({ "x-served-by": "node-server" });
        call.setTrailer({ "x-duration-ms": "42" });
        return { value: "" };
      },
    });

    const entry = registry.lookup("/echo.v1.Echo/Echo")!;
    const call: UnaryServerCall<Req> = {
      metadata: {},
      sendHeader,
      setTrailer,
      request: { value: "" },
    };
    await (entry.handler as UnaryHandler<Req, Res>)(call);

    expect(sendHeader).toHaveBeenCalledWith({ "x-served-by": "node-server" });
    expect(setTrailer).toHaveBeenCalledWith({ "x-duration-ms": "42" });
  });
});

describe("ServiceRegistry — server-streaming dispatch", () => {
  it("handler writes multiple responses via call.write()", async () => {
    const registry = new ServiceRegistry();
    const handler: ServerStreamHandler<Req, Res> = async (call) => {
      for (let i = 0; i < 3; i++) {
        call.write({ value: `${call.request.value}:${i}` });
      }
    };
    registry.addService(EchoService, { echoStream: handler });

    const entry = registry.lookup("/echo.v1.Echo/EchoStream")!;
    const written: Res[] = [];
    const call: ServerStreamCall<Req, Res> = {
      ...noopBase,
      request: { value: "ping" },
      write: (r) => written.push(r),
    };

    await (entry.handler as ServerStreamHandler<Req, Res>)(call);

    expect(written).toEqual([
      { value: "ping:0" },
      { value: "ping:1" },
      { value: "ping:2" },
    ]);
  });

  it("handler that writes nothing completes without error", async () => {
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echoStream: async (_call) => { /* intentionally empty */ },
    });

    const entry = registry.lookup("/echo.v1.Echo/EchoStream")!;
    const call: ServerStreamCall<Req, Res> = {
      ...noopBase,
      request: { value: "" },
      write: vi.fn(),
    };
    await expect(
      (entry.handler as ServerStreamHandler<Req, Res>)(call),
    ).resolves.toBeUndefined();
  });
});

describe("ServiceRegistry — client-streaming dispatch", () => {
  it("handler iterates all incoming messages and returns one response", async () => {
    const registry = new ServiceRegistry();
    const handler: ClientStreamHandler<Req, Res> = async (call) => {
      const parts: string[] = [];
      for await (const msg of call) {
        parts.push(msg.value);
      }
      return { value: parts.join(",") };
    };
    registry.addService(EchoService, { echoCollect: handler });

    const entry = registry.lookup("/echo.v1.Echo/EchoCollect")!;
    const msgs = fromArray<Req>([{ value: "a" }, { value: "b" }, { value: "c" }]);
    const call: ClientStreamCall<Req> = {
      ...noopBase,
      [Symbol.asyncIterator]: () => msgs[Symbol.asyncIterator](),
    };

    const result = await (entry.handler as ClientStreamHandler<Req, Res>)(call);
    expect(result.value).toBe("a,b,c");
  });

  it("handler tolerates empty message stream", async () => {
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echoCollect: async (call) => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _msg of call) { /* drain */ }
        return { value: "empty" };
      },
    });

    const entry = registry.lookup("/echo.v1.Echo/EchoCollect")!;
    const call: ClientStreamCall<Req> = {
      ...noopBase,
      [Symbol.asyncIterator]: () => fromArray<Req>([])[Symbol.asyncIterator](),
    };
    const result = await (entry.handler as ClientStreamHandler<Req, Res>)(call);
    expect(result.value).toBe("empty");
  });
});

describe("ServiceRegistry — bidi-streaming dispatch", () => {
  it("handler iterates incoming messages and writes responses", async () => {
    const registry = new ServiceRegistry();
    const handler: BidiHandler<Req, Res> = async (call) => {
      for await (const msg of call) {
        call.write({ value: `echo:${msg.value}` });
      }
    };
    registry.addService(EchoService, { echoBidi: handler });

    const entry = registry.lookup("/echo.v1.Echo/EchoBidi")!;
    const incoming = fromArray<Req>([{ value: "x" }, { value: "y" }, { value: "z" }]);
    const written: Res[] = [];
    const call: BidiCall<Req, Res> = {
      ...noopBase,
      write: (r) => written.push(r),
      [Symbol.asyncIterator]: () => incoming[Symbol.asyncIterator](),
    };

    await (entry.handler as BidiHandler<Req, Res>)(call);

    expect(written).toEqual([
      { value: "echo:x" },
      { value: "echo:y" },
      { value: "echo:z" },
    ]);
  });

  it("handler can write before the iterator is exhausted (simulated concurrency)", async () => {
    // Verifies the call shape supports interleaved write() during iteration —
    // important because real bidi handlers often write before fully reading.
    const registry = new ServiceRegistry();
    registry.addService(EchoService, {
      echoBidi: async (call) => {
        let count = 0;
        for await (const msg of call) {
          call.write({ value: `${msg.value}:${count++}` });
        }
      },
    });

    const entry = registry.lookup("/echo.v1.Echo/EchoBidi")!;
    const incoming = fromArray<Req>([{ value: "a" }, { value: "b" }]);
    const written: Res[] = [];
    const call: BidiCall<Req, Res> = {
      ...noopBase,
      write: (r) => written.push(r),
      [Symbol.asyncIterator]: () => incoming[Symbol.asyncIterator](),
    };

    await (entry.handler as BidiHandler<Req, Res>)(call);
    expect(written).toHaveLength(2);
    expect(written[0].value).toBe("a:0");
    expect(written[1].value).toBe("b:1");
  });
});
