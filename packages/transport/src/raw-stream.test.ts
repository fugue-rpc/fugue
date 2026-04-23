import { describe, expect, it } from "vitest";
import { GrpcStatusError, RawStream } from "./raw-stream.js";
import { FrameType } from "./frame.js";

// Builds a RawStream wired to a mock write function.
function makeStream() {
  const written: Array<{ type: number; streamId: number; payload: Uint8Array }> = [];
  const stream = new RawStream(
    1,
    (type, streamId, payload) => written.push({ type, streamId, payload }),
    () => {},
  );
  return { stream, written };
}

// Drains all yielded values from an async iterator (up to a limit to guard
// against infinite loops in tests).
async function collect<T>(
  iter: AsyncIterator<T>,
  limit = 100,
): Promise<T[]> {
  const results: T[] = [];
  for (let i = 0; i < limit; i++) {
    const r = await iter.next();
    if (r.done) break;
    results.push(r.value);
  }
  return results;
}

// ── Isolation test 1 ──────────────────────────────────────────────────────────
// 10 sends interleaved with reads: no lost messages, correct order.
describe("send + deliver interleaving", () => {
  it("delivers 10 messages in order", async () => {
    const { stream } = makeStream();
    const iter = stream[Symbol.asyncIterator]();

    const payloads = Array.from({ length: 10 }, (_, i) =>
      new Uint8Array([i]),
    );

    // Fire all delivers before awaiting any next() calls.
    for (const p of payloads) {
      stream._deliver(p);
    }
    stream._serverClose(0, "", {});

    const received = await collect(iter);
    expect(received).toHaveLength(10);
    for (let i = 0; i < 10; i++) {
      expect(received[i]).toEqual(payloads[i]);
    }
  });

  it("delivers messages that arrive after next() is already awaiting", async () => {
    const { stream } = makeStream();
    const iter = stream[Symbol.asyncIterator]();

    const nextP = iter.next(); // suspended, no messages yet
    stream._deliver(new Uint8Array([42]));
    const r = await nextP;
    expect(r.done).toBe(false);
    expect(r.value).toEqual(new Uint8Array([42]));
  });
});

// ── Isolation test 2 ──────────────────────────────────────────────────────────
// iterator.return() while consumer is suspended:
//   - RESET sent synchronously
//   - iterator resolves { done: true }
//   - subsequent send() throws
describe("iterator.return() cancels the stream", () => {
  it("sends RESET, resolves the pending next(), then send() throws", async () => {
    const { stream, written } = makeStream();
    const iter = stream[Symbol.asyncIterator]();

    const nextP = iter.next(); // suspended at await

    // Return the iterator (simulates for-await break / React unmount).
    const retP = iter.return!();

    // RESET must have been written synchronously before any microtask.
    expect(written.some((f) => f.type === FrameType.RESET)).toBe(true);

    // Both promises resolve with done: true.
    const [nextResult, retResult] = await Promise.all([nextP, retP]);
    expect(nextResult.done).toBe(true);
    expect(retResult.done).toBe(true);

    // Stream is now closed; send() must throw.
    expect(() => stream._sendMsg(new Uint8Array([1]))).toThrow("wsgrpc: stream closed");
  });
});

// ── Isolation test 3 ──────────────────────────────────────────────────────────
// Server RESET arriving before consumer reads: iterator terminates cleanly.
describe("server-initiated RESET before consumer reads", () => {
  it("iterator terminates with done:true, no error thrown", async () => {
    const { stream } = makeStream();

    stream._reset(); // server sends RESET before client reads anything

    const iter = stream[Symbol.asyncIterator]();
    const r = await iter.next();
    expect(r.done).toBe(true);
  });

  it("send() before _reset() does not throw; send() after throws", () => {
    const { stream } = makeStream();

    // send() while still OPEN — must not throw (racing with the incoming RESET)
    expect(() => stream._sendMsg(new Uint8Array([1]))).not.toThrow();

    stream._reset();

    // send() after RESET — must throw
    expect(() => stream._sendMsg(new Uint8Array([2]))).toThrow("wsgrpc: stream closed");
  });
});

// ── Isolation test 4 ──────────────────────────────────────────────────────────
// send() after the stream is explicitly cancelled.
describe("send() after cancel()", () => {
  it("throws synchronously", () => {
    const { stream } = makeStream();
    stream.cancel();
    expect(() => stream._sendMsg(new Uint8Array([1]))).toThrow("wsgrpc: stream closed");
  });

  it("cancel() sends RESET exactly once (idempotent)", () => {
    const { stream, written } = makeStream();
    stream.cancel();
    stream.cancel();
    const resets = written.filter((f) => f.type === FrameType.RESET);
    expect(resets).toHaveLength(1);
  });
});

// ── gRPC error status propagation ─────────────────────────────────────────────
describe("gRPC error status from server END", () => {
  it("iterator rejects with GrpcStatusError when status code is non-zero", async () => {
    const { stream } = makeStream();
    stream._serverClose(5, "not found", { "x-trace": "abc" });

    const iter = stream[Symbol.asyncIterator]();
    await expect(iter.next()).rejects.toBeInstanceOf(GrpcStatusError);

    try {
      await iter.next();
    } catch (e) {
      expect(e).toBeInstanceOf(GrpcStatusError);
      expect((e as GrpcStatusError).code).toBe(5);
      expect((e as GrpcStatusError).message).toBe("not found");
      expect((e as GrpcStatusError).trailers["x-trace"]).toBe("abc");
    }
  });

  it("messages already in queue are delivered before the error", async () => {
    const { stream } = makeStream();
    stream._deliver(new Uint8Array([1]));
    stream._deliver(new Uint8Array([2]));
    stream._serverClose(14, "unavailable", {});

    const iter = stream[Symbol.asyncIterator]();
    const r1 = await iter.next();
    expect(r1.done).toBe(false);
    expect(r1.value).toEqual(new Uint8Array([1]));

    const r2 = await iter.next();
    expect(r2.done).toBe(false);
    expect(r2.value).toEqual(new Uint8Array([2]));

    await expect(iter.next()).rejects.toBeInstanceOf(GrpcStatusError);
  });
});

// ── Unary call factory ─────────────────────────────────────────────────────────
describe("RawStream.unary()", () => {
  it("sends req MSG + END then resolves with decoded response", async () => {
    const { stream, written } = makeStream();
    const reqBytes = new Uint8Array([0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f]);
    const respBytes = new Uint8Array([0x0a, 0x05, 0x77, 0x6f, 0x72, 0x6c, 0x64]);

    const call = stream.unary(reqBytes, (b) => b);

    expect(written[0]?.type).toBe(FrameType.MSG);
    expect(written[1]?.type).toBe(FrameType.END); // client half-close

    // Simulate server response.
    stream._deliver(respBytes);
    stream._serverClose(0, "", {});

    const result = await call;
    expect(result).toEqual(respBytes);
  });

  it("rejects if server sends END with no MSG", async () => {
    const { stream } = makeStream();
    const call = stream.unary(new Uint8Array(0), (b) => b);
    stream._serverClose(0, "", {});
    await expect(Promise.resolve(call)).rejects.toThrow("no response");
  });
});

// ── BidiStream factory ────────────────────────────────────────────────────────
describe("RawStream.bidiStream()", () => {
  it("send() encodes and writes MSG, halfClose() writes END", () => {
    const { stream, written } = makeStream();
    const bidi = stream.bidiStream<string, string>(
      (s) => new TextEncoder().encode(s),
      (b) => new TextDecoder().decode(b),
    );

    bidi.send("hello");
    bidi.send("world");
    bidi.halfClose();

    const msgs = written.filter((f) => f.type === FrameType.MSG);
    expect(msgs).toHaveLength(2);
    expect(written.at(-1)?.type).toBe(FrameType.END);
  });

  it("iterates decoded responses until server closes", async () => {
    const { stream } = makeStream();
    const bidi = stream.bidiStream<string, string>(
      (s) => new TextEncoder().encode(s),
      (b) => new TextDecoder().decode(b),
    );

    stream._deliver(new TextEncoder().encode("echo:a"));
    stream._deliver(new TextEncoder().encode("echo:b"));
    stream._serverClose(0, "", {});

    const msgs: string[] = [];
    for await (const m of bidi) {
      msgs.push(m);
    }
    expect(msgs).toEqual(["echo:a", "echo:b"]);
  });
});

// ── ClientStream factory ──────────────────────────────────────────────────────
describe("RawStream.clientStream()", () => {
  it("send() + closeAndReceive() sends END and awaits single response", async () => {
    const { stream, written } = makeStream();
    const cs = stream.clientStream<string, string>(
      (s) => new TextEncoder().encode(s),
      (b) => new TextDecoder().decode(b),
    );

    cs.send("a");
    cs.send("b");

    // closeAndReceive sends END then waits for server's response.
    const resultP = cs.closeAndReceive();
    expect(written.at(-1)?.type).toBe(FrameType.END);

    stream._deliver(new TextEncoder().encode("a,b"));
    stream._serverClose(0, "", {});

    expect(await resultP).toBe("a,b");
  });
});

// ── ServerStream factory ──────────────────────────────────────────────────────
describe("RawStream.serverStream()", () => {
  it("sends req + END then iterates responses", async () => {
    const { stream, written } = makeStream();
    const ss = stream.serverStream(
      new Uint8Array([1]),
      (b) => b[0],
    );

    expect(written[0]?.type).toBe(FrameType.MSG);
    expect(written[1]?.type).toBe(FrameType.END);

    stream._deliver(new Uint8Array([10]));
    stream._deliver(new Uint8Array([20]));
    stream._serverClose(0, "", {});

    const results: number[] = [];
    for await (const n of ss) {
      results.push(n);
    }
    expect(results).toEqual([10, 20]);
  });
});

