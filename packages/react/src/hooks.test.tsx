/// <reference types="vitest" />
import { describe, expect, it, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";
import React from "react";
import { FugueProvider, useTransport } from "./context.js";
import { useUnary } from "./use-unary.js";
import { useServerStream } from "./use-server-stream.js";
import { useBidiStream } from "./use-bidi-stream.js";
import type { BidiStream, ServerStream, UnaryCall } from "@fugue-rpc/transport";
import { GrpcStatusError } from "@fugue-rpc/transport";

// ── Minimal fake transport / stream helpers ───────────────────────────────────

function fakeTransport() {
  return {} as ReturnType<typeof useTransport>;
}

function makeUnaryCall<Res>(
  result: Promise<Res>,
): UnaryCall<Res> {
  const cancel = vi.fn();
  return {
    then(onF, onR) { return result.then(onF, onR); },
    cancel,
  };
}

function makeServerStream<Res>(messages: Res[], error?: Error): ServerStream<Res> {
  const cancel = vi.fn();
  async function* gen() {
    for (const m of messages) yield m;
    if (error) throw error;
  }
  const iter = gen();
  return {
    cancel,
    [Symbol.asyncIterator]() { return iter; },
  };
}

function makeBidiStream<Req, Res>(
  serverMessages: Res[],
  error?: Error,
): BidiStream<Req, Res> & { sent: Req[]; halfCloseCount: number } {
  const sent: Req[] = [];
  let halfCloseCount = 0;
  const cancel = vi.fn();

  async function* gen() {
    for (const m of serverMessages) yield m;
    if (error) throw error;
  }
  const iter = gen();

  return {
    sent,
    halfCloseCount,
    send(req: Req) { sent.push(req); },
    halfClose() { halfCloseCount++; },
    cancel,
    [Symbol.asyncIterator]() { return iter; },
  };
}

// ── Context ───────────────────────────────────────────────────────────────────
describe("FugueProvider / useTransport", () => {
  it("throws when used outside provider", () => {
    expect(() =>
      renderHook(() => useTransport()),
    ).toThrow("useTransport must be used inside <FugueProvider>");
  });

  it("provides the transport via context", () => {
    const t = fakeTransport();
    const { result } = renderHook(() => useTransport(), {
      wrapper: ({ children }) => (
        <FugueProvider transport={t as never}>{children}</FugueProvider>
      ),
    });
    expect(result.current).toBe(t);
  });
});

// ── useUnary ──────────────────────────────────────────────────────────────────
describe("useUnary", () => {
  it("starts idle, transitions loading → success", async () => {
    let resolve!: (v: string) => void;
    const p = new Promise<string>((r) => (resolve = r));

    const callFn = vi.fn(() => makeUnaryCall(p));
    const { result } = renderHook(() => useUnary(callFn));

    expect(result.current.state.status).toBe("idle");

    act(() => result.current.execute("req"));
    expect(result.current.state.status).toBe("loading");

    await act(async () => resolve("hello"));
    expect(result.current.state).toEqual({ status: "success", data: "hello" });
  });

  it("transitions loading → error on rejection", async () => {
    const err = new GrpcStatusError(5, "not found", {});
    const callFn = vi.fn(() => makeUnaryCall(Promise.reject(err)));
    const { result } = renderHook(() => useUnary(callFn));

    await act(async () => result.current.execute("req"));

    expect(result.current.state.status).toBe("error");
    if (result.current.state.status === "error") {
      expect(result.current.state.error).toBe(err);
    }
  });

  it("reset() returns to idle and cancels in-flight call", async () => {
    const p = new Promise<string>(() => {}); // never resolves
    const call = makeUnaryCall(p);
    const callFn = vi.fn(() => call);
    const { result } = renderHook(() => useUnary(callFn));

    act(() => result.current.execute("req"));
    expect(result.current.state.status).toBe("loading");

    act(() => result.current.reset());
    expect(result.current.state.status).toBe("idle");
    expect(call.cancel).toHaveBeenCalled();
  });
});

// ── useServerStream ───────────────────────────────────────────────────────────
describe("useServerStream", () => {
  it("accumulates messages then transitions to done", async () => {
    const callFn = vi.fn(() => makeServerStream(["a", "b", "c"]));
    const { result } = renderHook(() => useServerStream(callFn));

    expect(result.current.state.status).toBe("idle");

    await act(async () => result.current.start("req"));

    expect(result.current.state).toEqual({
      status: "done",
      messages: ["a", "b", "c"],
    });
  });

  it("transitions to error state when server throws GrpcStatusError", async () => {
    const err = new GrpcStatusError(14, "unavailable", {});
    const callFn = vi.fn(() => makeServerStream(["partial"], err));
    const { result } = renderHook(() => useServerStream(callFn));

    await act(async () => result.current.start("req"));

    expect(result.current.state.status).toBe("error");
    if (result.current.state.status === "error") {
      expect(result.current.state.messages).toEqual(["partial"]);
      expect(result.current.state.error).toBe(err);
    }
  });

  it("reset() cancels stream and returns to idle", async () => {
    const stream = makeServerStream<string>([]); // empty, hangs waiting
    const callFn = vi.fn(() => stream);
    const { result } = renderHook(() => useServerStream(callFn));

    act(() => result.current.start("req"));
    act(() => result.current.reset());

    expect(result.current.state.status).toBe("idle");
    expect(stream.cancel).toHaveBeenCalled();
  });
});

// ── useBidiStream ─────────────────────────────────────────────────────────────
describe("useBidiStream", () => {
  it("open() → send() → halfClose() → done with server messages", async () => {
    const stream = makeBidiStream<string, string>(["echo:a", "echo:b"]);
    const callFn = vi.fn(() => stream);
    const { result } = renderHook(() => useBidiStream(callFn));

    await act(async () => {
      result.current.open();
      result.current.send("a");
      result.current.send("b");
      result.current.halfClose();
    });

    expect(stream.sent).toEqual(["a", "b"]);
    expect(result.current.state).toEqual({
      status: "done",
      messages: ["echo:a", "echo:b"],
    });
  });

  it("transitions to error when server sends gRPC error", async () => {
    const err = new GrpcStatusError(2, "unknown", {});
    const stream = makeBidiStream<string, string>([], err);
    const callFn = vi.fn(() => stream);
    const { result } = renderHook(() => useBidiStream(callFn));

    await act(async () => result.current.open());

    expect(result.current.state.status).toBe("error");
    if (result.current.state.status === "error") {
      expect(result.current.state.error).toBe(err);
    }
  });

  it("cancel() stops the stream and leaves state as-is", async () => {
    const stream = makeBidiStream<string, string>([]);
    const callFn = vi.fn(() => stream);
    const { result } = renderHook(() => useBidiStream(callFn));

    act(() => result.current.open());
    act(() => result.current.cancel());

    expect(stream.cancel).toHaveBeenCalled();
  });

  it("cancel() immediately after open() never transitions to 'done'", async () => {
    const stream = makeBidiStream<string, string>([]);
    const callFn = vi.fn(() => stream);
    const { result } = renderHook(() => useBidiStream(callFn));

    act(() => {
      result.current.open();
      result.current.cancel(); // sync cancel before async reader can finish
    });

    // Let any pending microtasks drain.
    await act(async () => {});

    expect(result.current.state.status).not.toBe("done");
    expect(stream.cancel).toHaveBeenCalled();
  });

  it("open() is a no-op when stream is already open", () => {
    let callCount = 0;
    const callFn = () => {
      callCount++;
      return makeBidiStream<string, string>([]);
    };
    const { result } = renderHook(() => useBidiStream(callFn));

    act(() => {
      result.current.open();
      result.current.open(); // second call must be ignored
    });

    expect(callCount).toBe(1);
  });

  it("open(initialRequest) sends the request immediately", async () => {
    const stream = makeBidiStream<string, string>(["echo:init"]);
    const callFn = vi.fn(() => stream);
    const { result } = renderHook(() => useBidiStream(callFn));

    await act(async () => result.current.open("init"));

    expect(stream.sent).toContain("init");
  });
});
