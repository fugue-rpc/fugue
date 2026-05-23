// React hooks e2e stress test — requires echo server on :8080
// Run with: pnpm test:e2e  (echo server started automatically via globalSetup)
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import React from "react";
import { FugueTransport } from "@fugue-rpc/transport";
import { FugueProvider } from "./context.js";
import { useUnary } from "./use-unary.js";
import { useServerStream } from "./use-server-stream.js";
import { useBidiStream } from "./use-bidi-stream.js";

const SERVER = "ws://localhost:8080/fugue/";
const UNARY_METHOD = "/echo.v1.Echo/Echo";
const SS_METHOD = "/echo.v1.Echo/EchoStream";
const BIDI_METHOD = "/echo.v1.Echo/EchoBidi";

// Minimal proto codec for echo.v1.Msg { string value = 1 }
function encodeMsg(value: string): Uint8Array {
  const enc = new TextEncoder().encode(value);
  const out = new Uint8Array(2 + enc.length);
  out[0] = 0x0a;
  out[1] = enc.length;
  out.set(enc, 2);
  return out;
}
function decodeMsg(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes.slice(2));
}

// ── Correctness smoke tests ────────────────────────────────────────────────────

describe("React hooks e2e — correctness", () => {
  let transport: FugueTransport;

  beforeEach(() => { transport = new FugueTransport(SERVER); });
  afterEach(() => { transport.close(); });

  function wrapper({ children }: { children: React.ReactNode }) {
    return <FugueProvider transport={transport}>{children}</FugueProvider>;
  }

  it("useUnary: request echoed back", async () => {
    const callFn = (req: Uint8Array) =>
      transport.openStream(UNARY_METHOD).unary(req, decodeMsg);
    const { result } = renderHook(() => useUnary(callFn), { wrapper });

    act(() => result.current.execute(encodeMsg("hello")));
    await waitFor(() => expect(result.current.state.status).toBe("success"), { timeout: 5000 });

    expect(result.current.state.status === "success" && result.current.state.data).toBe("hello");
  });

  it("useServerStream: receives 5 messages then done", async () => {
    const callFn = (req: Uint8Array) =>
      transport.openStream(SS_METHOD).serverStream(req, decodeMsg);
    const { result } = renderHook(() => useServerStream(callFn), { wrapper });

    act(() => result.current.start(encodeMsg("ping")));
    await waitFor(() => expect(result.current.state.status).toBe("done"), { timeout: 10_000 });

    expect(result.current.state.status === "done" && result.current.state.messages).toEqual(
      ["ping", "ping", "ping", "ping", "ping"],
    );
  });

  it("useBidiStream: send 3 messages, receive 3 echoes", async () => {
    const callFn = () =>
      transport.openStream(BIDI_METHOD).bidiStream(encodeMsg, decodeMsg);
    const { result } = renderHook(() => useBidiStream(callFn), { wrapper });

    act(() => {
      result.current.open();
      result.current.send("x");
      result.current.send("y");
      result.current.send("z");
      result.current.halfClose();
    });
    await waitFor(() => expect(result.current.state.status).toBe("done"), { timeout: 10_000 });

    expect(result.current.state.status === "done" && result.current.state.messages).toEqual(
      ["x", "y", "z"],
    );
  });
});

// ── Stress / throughput tests ─────────────────────────────────────────────────

describe("React hooks e2e — stress", () => {
  let transport: FugueTransport;

  beforeEach(() => { transport = new FugueTransport(SERVER); });
  afterEach(() => { transport.close(); });

  function wrapper({ children }: { children: React.ReactNode }) {
    return <FugueProvider transport={transport}>{children}</FugueProvider>;
  }

  it("useUnary: 50 sequential calls, report RPC/s", async () => {
    const callFn = (req: Uint8Array) =>
      transport.openStream(UNARY_METHOD).unary(req, decodeMsg);
    const { result } = renderHook(() => useUnary(callFn), { wrapper });

    const N = 50;
    const start = performance.now();

    for (let i = 0; i < N; i++) {
      act(() => result.current.execute(encodeMsg(`msg-${i}`)));
      await waitFor(() => expect(result.current.state.status).toBe("success"), { timeout: 5000 });
      const data = result.current.state.status === "success" ? result.current.state.data : null;
      expect(data).toBe(`msg-${i}`);
      act(() => result.current.reset());
    }

    const elapsed = performance.now() - start;
    const rps = Math.round(N / elapsed * 1000);
    console.log(`\n  useUnary stress: ${N} calls in ${elapsed.toFixed(0)}ms → ${rps} RPC/s`);
    expect(rps).toBeGreaterThan(0);
  }, 120_000);

  it("useServerStream: 10 concurrent streams via separate hook instances", async () => {
    const N = 10;
    const callFn = (req: Uint8Array) =>
      transport.openStream(SS_METHOD).serverStream(req, decodeMsg);

    const start = performance.now();

    const promises = Array.from({ length: N }, async (_, i) => {
      const { result, unmount } = renderHook(() => useServerStream(callFn), { wrapper });
      act(() => result.current.start(encodeMsg(`item-${i}`)));
      await waitFor(() => expect(result.current.state.status).toBe("done"), { timeout: 15_000 });
      const msgs = result.current.state.status === "done" ? result.current.state.messages : [];
      unmount();
      return msgs;
    });

    const allResults = await Promise.all(promises);
    const elapsed = performance.now() - start;
    const totalMsgs = allResults.reduce((s, m) => s + m.length, 0);
    const msgps = Math.round(totalMsgs / elapsed * 1000);
    console.log(`\n  useServerStream stress: ${N} concurrent streams, ${totalMsgs} msgs total in ${elapsed.toFixed(0)}ms → ${msgps} msg/s`);

    for (const msgs of allResults) {
      expect(msgs).toHaveLength(5);
    }
  }, 120_000);

  it("useBidiStream: 5 concurrent bidi streams", async () => {
    const N = 5;
    const MSGS_PER_STREAM = 5;
    const callFn = () =>
      transport.openStream(BIDI_METHOD).bidiStream(encodeMsg, decodeMsg);

    const start = performance.now();

    const promises = Array.from({ length: N }, async (_, i) => {
      const { result, unmount } = renderHook(() => useBidiStream(callFn), { wrapper });

      act(() => {
        result.current.open();
        for (let m = 0; m < MSGS_PER_STREAM; m++) {
          result.current.send(`stream-${i}-msg-${m}`);
        }
        result.current.halfClose();
      });

      await waitFor(() => expect(result.current.state.status).toBe("done"), { timeout: 15_000 });
      const msgs = result.current.state.status === "done" ? result.current.state.messages : [];
      unmount();
      return msgs;
    });

    const allResults = await Promise.all(promises);
    const elapsed = performance.now() - start;
    const totalMsgs = allResults.reduce((s, m) => s + m.length, 0);
    const msgps = Math.round(totalMsgs / elapsed * 1000);
    console.log(`\n  useBidiStream stress: ${N} concurrent bidi streams, ${totalMsgs} msgs total in ${elapsed.toFixed(0)}ms → ${msgps} msg/s`);

    for (let i = 0; i < N; i++) {
      expect(allResults[i]).toHaveLength(MSGS_PER_STREAM);
      for (let m = 0; m < MSGS_PER_STREAM; m++) {
        expect(allResults[i][m]).toBe(`stream-${i}-msg-${m}`);
      }
    }
  }, 120_000);
});
