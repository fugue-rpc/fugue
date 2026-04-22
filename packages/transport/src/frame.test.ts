import { describe, expect, it } from "vitest";
import {
  decodeFrame,
  encodeFrame,
  FrameType,
  HEADER_SIZE,
  MAX_PAYLOAD_SIZE,
} from "./frame.js";

const ALL_TYPES = [
  ["BEGIN",  FrameType.BEGIN],
  ["MSG",    FrameType.MSG],
  ["END",    FrameType.END],
  ["RESET",  FrameType.RESET],
  ["HEADER", FrameType.HEADER],
] as const;

describe("round-trip", () => {
  const payload = new TextEncoder().encode("hello grpcws");

  for (const [name, type] of ALL_TYPES) {
    it(name, () => {
      const orig = { type, streamId: 42, payload };
      const encoded = encodeFrame(orig);
      expect(encoded.length).toBe(HEADER_SIZE + payload.length);

      const got = decodeFrame(encoded);
      expect(got.type).toBe(orig.type);
      expect(got.streamId).toBe(orig.streamId);
      expect(got.payload).toEqual(orig.payload);
    });
  }
});

it("empty payload round-trips", () => {
  const orig = { type: FrameType.HEADER, streamId: 1, payload: new Uint8Array(0) };
  const encoded = encodeFrame(orig);
  expect(encoded.length).toBe(HEADER_SIZE);

  const got = decodeFrame(encoded);
  expect(got.payload.length).toBe(0);
});

it("header byte layout matches wire-format.md §2", () => {
  const payload = new Uint8Array([0xab, 0xcd]);
  const encoded = encodeFrame({ type: FrameType.MSG, streamId: 0x01020304, payload });

  expect(encoded[0]).toBe(FrameType.MSG);           // byte 0: type
  expect(encoded[1]).toBe(0x01);                    // bytes 1-4: stream_id big-endian
  expect(encoded[2]).toBe(0x02);
  expect(encoded[3]).toBe(0x03);
  expect(encoded[4]).toBe(0x04);
  expect(encoded[5]).toBe(0x00);                    // bytes 5-8: payload_length big-endian
  expect(encoded[6]).toBe(0x00);
  expect(encoded[7]).toBe(0x00);
  expect(encoded[8]).toBe(0x02);
  expect(encoded[9]).toBe(0xab);                    // payload
  expect(encoded[10]).toBe(0xcd);
});

it("max uint32 stream ID round-trips", () => {
  const f = { type: FrameType.MSG, streamId: 0xffffffff, payload: new Uint8Array([1]) };
  expect(decodeFrame(encodeFrame(f)).streamId).toBe(0xffffffff);
});

it("extra trailing bytes are ignored", () => {
  const orig = { type: FrameType.MSG, streamId: 1, payload: new Uint8Array([0xaa, 0xbb]) };
  const encoded = encodeFrame(orig);
  const withTrailing = new Uint8Array([...encoded, 0xff, 0xff]);

  const got = decodeFrame(withTrailing);
  expect(got.payload).toEqual(orig.payload);
});

it("decodeFrame works on a subarray with non-zero byteOffset", () => {
  const orig = { type: FrameType.END, streamId: 7, payload: new Uint8Array([1, 2, 3]) };
  const full = new Uint8Array([0xff, 0xff, ...encodeFrame(orig)]);
  const sub = full.subarray(2); // non-zero byteOffset

  const got = decodeFrame(sub);
  expect(got.type).toBe(orig.type);
  expect(got.streamId).toBe(orig.streamId);
  expect(got.payload).toEqual(orig.payload);
});

describe("encode errors", () => {
  it("rejects payload exceeding MAX_PAYLOAD_SIZE", () => {
    const oversized = new Uint8Array(MAX_PAYLOAD_SIZE + 1);
    expect(() => encodeFrame({ type: FrameType.MSG, streamId: 1, payload: oversized }))
      .toThrow("MAX_PAYLOAD_SIZE");
  });
});

describe("decode errors", () => {
  it("rejects buffer shorter than header", () => {
    expect(() => decodeFrame(new Uint8Array([0x02, 0x00, 0x00])))
      .toThrow("too short for header");
  });

  it("rejects declared payload length exceeding MAX_PAYLOAD_SIZE", () => {
    const buf = new Uint8Array(HEADER_SIZE);
    const view = new DataView(buf.buffer);
    view.setUint8(0, FrameType.MSG);
    view.setUint32(1, 1, false);
    view.setUint32(5, MAX_PAYLOAD_SIZE + 1, false);

    expect(() => decodeFrame(buf)).toThrow("MAX_PAYLOAD_SIZE");
  });

  it("rejects buffer shorter than declared payload", () => {
    const buf = new Uint8Array(HEADER_SIZE + 5); // declares 10, provides 5
    const view = new DataView(buf.buffer);
    view.setUint8(0, FrameType.MSG);
    view.setUint32(1, 1, false);
    view.setUint32(5, 10, false);

    expect(() => decodeFrame(buf)).toThrow("too short for declared payload");
  });
});

it("MAX_PAYLOAD_SIZE exactly at limit is accepted", () => {
  const payload = new Uint8Array(MAX_PAYLOAD_SIZE);
  payload[0] = 0xde;
  payload[MAX_PAYLOAD_SIZE - 1] = 0xad;

  const encoded = encodeFrame({ type: FrameType.MSG, streamId: 1, payload });
  const got = decodeFrame(encoded);
  expect(got.payload[0]).toBe(0xde);
  expect(got.payload[MAX_PAYLOAD_SIZE - 1]).toBe(0xad);
});
