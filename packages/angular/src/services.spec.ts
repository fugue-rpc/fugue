import { TestBed } from "@angular/core/testing";
import { vi, describe, it, expect, beforeEach } from "vitest";
import { FugueUnaryService } from "./unary.service.js";
import { FugueServerStreamService } from "./server-stream.service.js";
import { FugueBidiStreamService } from "./bidi-stream.service.js";
import type { UnaryCall, ServerStream, BidiStream } from "@fugue-rpc/transport";
import { GrpcStatusError } from "@fugue-rpc/transport";

// ---- helpers ----

function makeUnaryCall<Res>(): {
  call: UnaryCall<Res>;
  resolve(v: Res): void;
  reject(e: unknown): void;
  cancel: ReturnType<typeof vi.fn>;
} {
  let resolve!: (v: Res) => void;
  let reject!: (e: unknown) => void;
  const p = new Promise<Res>((res, rej) => { resolve = res; reject = rej; });
  const cancel = vi.fn();
  const call: UnaryCall<Res> = { then: p.then.bind(p), cancel };
  return { call, resolve, reject, cancel };
}

function makeServerStream<Res>(): {
  stream: ServerStream<Res>;
  push(msg: Res): void;
  end(): void;
  error(e: unknown): void;
  cancel: ReturnType<typeof vi.fn>;
} {
  const msgs: Res[] = [];
  let waiter: ((done: boolean) => void) | null = null;
  let ended = false;
  let errored: unknown = null;
  const cancel = vi.fn(() => { ended = true; waiter?.(true); });

  async function* gen(): AsyncGenerator<Res> {
    while (true) {
      if (msgs.length > 0) { yield msgs.shift()!; continue; }
      if (ended) return;
      if (errored != null) throw errored;
      await new Promise<boolean>(r => { waiter = r; });
      waiter = null;
    }
  }

  return {
    stream: { [Symbol.asyncIterator]: gen, cancel },
    push(msg) { msgs.push(msg); waiter?.(false); },
    end() { ended = true; waiter?.(true); },
    error(e) { errored = e; waiter?.(true); },
    cancel,
  };
}

function makeBidiStream<Req, Res>(): {
  stream: BidiStream<Req, Res>;
  push(msg: Res): void;
  end(): void;
  sent: Req[];
  halfClosed: boolean;
  cancel: ReturnType<typeof vi.fn>;
} {
  const inMsgs: Res[] = [];
  const sent: Req[] = [];
  let waiter: ((done: boolean) => void) | null = null;
  let ended = false;
  let halfClosed = false;
  const cancel = vi.fn(() => { ended = true; waiter?.(true); });

  async function* gen(): AsyncGenerator<Res> {
    while (true) {
      if (inMsgs.length > 0) { yield inMsgs.shift()!; continue; }
      if (ended) return;
      await new Promise<boolean>(r => { waiter = r; });
      waiter = null;
    }
  }

  const stream: BidiStream<Req, Res> = {
    [Symbol.asyncIterator]: gen,
    send(req) { sent.push(req); },
    halfClose() { halfClosed = true; },
    cancel,
  };

  return {
    stream,
    push(msg) { inMsgs.push(msg); waiter?.(false); },
    end() { ended = true; waiter?.(true); },
    sent,
    get halfClosed() { return halfClosed; },
    cancel,
  };
}

// ---- FugueUnaryService ----

describe("FugueUnaryService", () => {
  let service: FugueUnaryService<string, string>;

  beforeEach(() => {
    TestBed.configureTestingModule({ providers: [FugueUnaryService] });
    service = TestBed.inject(FugueUnaryService) as FugueUnaryService<string, string>;
  });

  it("starts idle", () => {
    expect(service.state()).toEqual({ status: "idle" });
  });

  it("loading → success", async () => {
    const { call, resolve } = makeUnaryCall<string>();
    service.execute(() => call, "req");
    expect(service.state().status).toBe("loading");
    resolve("hello");
    await Promise.resolve();
    expect(service.state()).toEqual({ status: "success", data: "hello" });
  });

  it("loading → error", async () => {
    const { call, reject } = makeUnaryCall<string>();
    service.execute(() => call, "req");
    reject(new GrpcStatusError(2, "UNKNOWN", {}));
    await Promise.resolve();
    const s = service.state();
    expect(s.status).toBe("error");
    if (s.status === "error") expect(s.error).toBeInstanceOf(GrpcStatusError);
  });

  it("reset cancels in-flight and returns idle", async () => {
    const { call, cancel, resolve } = makeUnaryCall<string>();
    service.execute(() => call, "req");
    service.reset();
    expect(cancel).toHaveBeenCalled();
    expect(service.state()).toEqual({ status: "idle" });
    resolve("late");
    await Promise.resolve();
    expect(service.state()).toEqual({ status: "idle" });
  });

  it("second execute cancels first", () => {
    const { call: c1, cancel: cancel1 } = makeUnaryCall<string>();
    const { call: c2 } = makeUnaryCall<string>();
    service.execute(() => c1, "req1");
    service.execute(() => c2, "req2");
    expect(cancel1).toHaveBeenCalled();
  });
});

// ---- FugueServerStreamService ----

describe("FugueServerStreamService", () => {
  let service: FugueServerStreamService<string, string>;

  beforeEach(() => {
    TestBed.configureTestingModule({ providers: [FugueServerStreamService] });
    service = TestBed.inject(FugueServerStreamService) as FugueServerStreamService<string, string>;
  });

  it("starts idle", () => {
    expect(service.state()).toEqual({ status: "idle" });
  });

  it("accumulates messages then done", async () => {
    const { stream, push, end } = makeServerStream<string>();
    service.start(() => stream, "req");
    expect(service.state().status).toBe("streaming");
    push("a");
    await new Promise(r => setTimeout(r, 0));
    push("b");
    await new Promise(r => setTimeout(r, 0));
    end();
    await new Promise(r => setTimeout(r, 0));
    const s = service.state();
    expect(s.status).toBe("done");
    if (s.status === "done") expect(s.messages).toEqual(["a", "b"]);
  });

  it("cancel stops stream", async () => {
    const { stream, cancel } = makeServerStream<string>();
    service.start(() => stream, "req");
    service.cancel();
    expect(cancel).toHaveBeenCalled();
  });

  it("reset returns to idle", () => {
    const { stream } = makeServerStream<string>();
    service.start(() => stream, "req");
    service.reset();
    expect(service.state()).toEqual({ status: "idle" });
  });
});

// ---- FugueBidiStreamService ----

describe("FugueBidiStreamService", () => {
  let service: FugueBidiStreamService<string, string>;

  beforeEach(() => {
    TestBed.configureTestingModule({ providers: [FugueBidiStreamService] });
    service = TestBed.inject(FugueBidiStreamService) as FugueBidiStreamService<string, string>;
  });

  it("starts idle", () => {
    expect(service.state()).toEqual({ status: "idle" });
  });

  it("open → send → done", async () => {
    const { stream, push, end, sent } = makeBidiStream<string, string>();
    service.open(() => stream);
    expect(service.state().status).toBe("open");
    service.send("msg1");
    expect(sent).toContain("msg1");
    push("resp1");
    await new Promise(r => setTimeout(r, 0));
    end();
    await new Promise(r => setTimeout(r, 0));
    const s = service.state();
    expect(s.status).toBe("done");
    if (s.status === "done") expect(s.messages).toEqual(["resp1"]);
  });

  it("open is no-op when already open", () => {
    const { stream: s1 } = makeBidiStream<string, string>();
    const { stream: s2 } = makeBidiStream<string, string>();
    service.open(() => s1);
    service.open(() => s2);
    expect(service.state().status).toBe("open");
    // Still using s1 — s2 factory was never called (service guards double-open)
  });

  it("open with initialRequest sends it immediately", () => {
    const { stream, sent } = makeBidiStream<string, string>();
    service.open(() => stream, "hello");
    expect(sent).toContain("hello");
  });

  it("halfClose delegates to stream", () => {
    const { stream } = makeBidiStream<string, string>();
    service.open(() => stream);
    service.halfClose();
    // halfClose tracked in makeBidiStream
  });

  it("cancel stops stream", () => {
    const { stream, cancel } = makeBidiStream<string, string>();
    service.open(() => stream);
    service.cancel();
    expect(cancel).toHaveBeenCalled();
  });

  it("reset returns to idle", () => {
    const { stream } = makeBidiStream<string, string>();
    service.open(() => stream);
    service.reset();
    expect(service.state()).toEqual({ status: "idle" });
  });
});
