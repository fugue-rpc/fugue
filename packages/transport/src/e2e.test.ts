// End-to-end tests: TypeScript transport ↔ real Go echo server.
// Run with: pnpm test:e2e  (starts echo-server automatically via globalSetup)
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { FugueTransport } from "./index.js";

const SERVER = "ws://localhost:8080/fugue/";

// Manual codec for echo.v1.Msg { string value = 1 }.
// Field 1, wire type 2 (length-delimited): tag byte 0x0a + varint length + UTF-8.
function encodeMsg(value: string): Uint8Array {
  const enc = new TextEncoder().encode(value);
  const out = new Uint8Array(2 + enc.length);
  out[0] = 0x0a;
  out[1] = enc.length;
  out.set(enc, 2);
  return out;
}
function decodeMsg(bytes: Uint8Array): string {
  // Skip tag (0x0a) and length byte.
  return new TextDecoder().decode(bytes.slice(2));
}

describe("E2E — fugue echo server", () => {
  let transport: FugueTransport;

  beforeEach(() => {
    transport = new FugueTransport(SERVER);
  });

  afterEach(() => {
    transport.close();
  });

  // ── Unary ──────────────────────────────────────────────────────────────────
  it("unary: request is echoed back", async () => {
    const result = await transport
      .openStream("/echo.v1.Echo/Echo")
      .unary(encodeMsg("hello"), decodeMsg);
    expect(result).toBe("hello");
  });

  it("unary: unicode payload round-trips correctly", async () => {
    const result = await transport
      .openStream("/echo.v1.Echo/Echo")
      .unary(encodeMsg("こんにちは 🌍"), decodeMsg);
    expect(result).toBe("こんにちは 🌍");
  });

  // ── Server streaming ───────────────────────────────────────────────────────
  it("server stream: receives exactly 5 echoes", async () => {
    const ss = transport
      .openStream("/echo.v1.Echo/EchoStream")
      .serverStream(encodeMsg("ping"), decodeMsg);
    const msgs: string[] = [];
    for await (const m of ss) msgs.push(m);
    expect(msgs).toEqual(["ping", "ping", "ping", "ping", "ping"]);
  });

  // ── Client streaming ───────────────────────────────────────────────────────
  it("client stream: collected values are joined with commas", async () => {
    const cs = transport
      .openStream("/echo.v1.Echo/EchoCollect")
      .clientStream((s: string) => encodeMsg(s), decodeMsg);
    cs.send("a");
    cs.send("b");
    cs.send("c");
    expect(await cs.closeAndReceive()).toBe("a,b,c");
  });

  // ── Bidi streaming ─────────────────────────────────────────────────────────
  it("bidi stream: each sent message is echoed in order", async () => {
    const bidi = transport
      .openStream("/echo.v1.Echo/EchoBidi")
      .bidiStream((s: string) => encodeMsg(s), decodeMsg);
    bidi.send("x");
    bidi.send("y");
    bidi.send("z");
    bidi.halfClose();
    const msgs: string[] = [];
    for await (const m of bidi) msgs.push(m);
    expect(msgs).toEqual(["x", "y", "z"]);
  });

  it("bidi stream: cancel() stops iteration cleanly", async () => {
    const bidi = transport
      .openStream("/echo.v1.Echo/EchoBidi")
      .bidiStream((s: string) => encodeMsg(s), decodeMsg);
    bidi.send("a");
    const iter = bidi[Symbol.asyncIterator]();
    // Read the first echo then cancel immediately.
    await iter.next(); // consume "a" echo
    const ret = await iter.return!();
    expect(ret.done).toBe(true);
  });

  // ── Multiple concurrent streams ────────────────────────────────────────────
  it("10 concurrent unary streams on one transport", async () => {
    const calls = Array.from({ length: 10 }, (_, i) =>
      transport
        .openStream("/echo.v1.Echo/Echo")
        .unary(encodeMsg(`msg-${i}`), decodeMsg),
    );
    const results = await Promise.all(calls.map((c) => Promise.resolve(c)));
    expect(results).toEqual(Array.from({ length: 10 }, (_, i) => `msg-${i}`));
  });
});
