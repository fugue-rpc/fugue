// Binary framing codec for the grpcws wire protocol.
// See docs/wire-format.md for the full specification.

export const FrameType = {
  BEGIN:  0x01,
  MSG:    0x02,
  END:    0x03,
  RESET:  0x04,
  HEADER: 0x06,
} as const satisfies Record<string, number>;

export type FrameTypeValue = (typeof FrameType)[keyof typeof FrameType];

export const HEADER_SIZE = 9;
export const MAX_PAYLOAD_SIZE = 4 * 1024 * 1024; // 4 MiB

export interface Frame {
  type: FrameTypeValue;
  streamId: number; // uint32
  payload: Uint8Array;
}

/**
 * Encodes a Frame into the 9-byte header + payload wire format.
 * Throws if payload.length > MAX_PAYLOAD_SIZE.
 */
export function encodeFrame(frame: Frame): Uint8Array {
  if (frame.payload.length > MAX_PAYLOAD_SIZE) {
    throw new Error(
      `frame: payload ${frame.payload.length} bytes exceeds MAX_PAYLOAD_SIZE (${MAX_PAYLOAD_SIZE})`,
    );
  }
  const buf = new Uint8Array(HEADER_SIZE + frame.payload.length);
  const view = new DataView(buf.buffer);
  view.setUint8(0, frame.type);
  view.setUint32(1, frame.streamId, false); // false = big-endian
  view.setUint32(5, frame.payload.length, false);
  buf.set(frame.payload, HEADER_SIZE);
  return buf;
}

/**
 * Decodes a complete frame from buf.
 * buf must contain at least the 9-byte header plus the declared payload length.
 * Extra trailing bytes are ignored.
 * Throws on short header, oversized payload, or short payload.
 */
export function decodeFrame(buf: Uint8Array): Frame {
  if (buf.length < HEADER_SIZE) {
    throw new Error(
      `frame: buffer too short for header (got ${buf.length} bytes, need ${HEADER_SIZE})`,
    );
  }
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const type = view.getUint8(0) as FrameTypeValue;
  const streamId = view.getUint32(1, false); // false = big-endian
  const payloadLength = view.getUint32(5, false);

  if (payloadLength > MAX_PAYLOAD_SIZE) {
    throw new Error(
      `frame: payload ${payloadLength} bytes exceeds MAX_PAYLOAD_SIZE (${MAX_PAYLOAD_SIZE})`,
    );
  }
  if (buf.length < HEADER_SIZE + payloadLength) {
    throw new Error(
      `frame: buffer too short for declared payload length (got ${buf.length} bytes, need ${HEADER_SIZE + payloadLength})`,
    );
  }
  return {
    type,
    streamId,
    payload: buf.slice(HEADER_SIZE, HEADER_SIZE + payloadLength),
  };
}
