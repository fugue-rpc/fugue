// Binary framing codec for the fugue wire protocol (Node.js / Buffer variant).
// See docs/wire-format.md for the full specification.

export const FrameType = {
  BEGIN:  0x01,
  MSG:    0x02,
  END:    0x03,
  RESET:  0x04,
  HEADER: 0x06,
} as const;

export const HEADER_SIZE = 9;
export const MAX_PAYLOAD_SIZE = 4 * 1024 * 1024; // 4 MiB

export interface Frame {
  type: number;
  streamId: number;
  payload: Buffer;
}

export function encodeFrame(type: number, streamId: number, payload: Uint8Array): Buffer {
  if (payload.length > MAX_PAYLOAD_SIZE) {
    throw new Error(
      `frame: payload ${payload.length} bytes exceeds MAX_PAYLOAD_SIZE (${MAX_PAYLOAD_SIZE})`,
    );
  }
  const buf = Buffer.allocUnsafe(HEADER_SIZE + payload.length);
  buf[0] = type;
  buf.writeUInt32BE(streamId, 1);
  buf.writeUInt32BE(payload.length, 5);
  if (payload.length > 0) buf.set(payload, HEADER_SIZE);
  return buf;
}

export function decodeAll(data: Buffer): Frame[] {
  const frames: Frame[] = [];
  let offset = 0;
  while (offset < data.length) {
    const remaining = data.length - offset;
    if (remaining < HEADER_SIZE) {
      throw new Error(
        `frame: buffer too short for header (${remaining} bytes remaining, need ${HEADER_SIZE})`,
      );
    }
    const type = data[offset];
    const streamId = data.readUInt32BE(offset + 1);
    const payloadLength = data.readUInt32BE(offset + 5);
    if (payloadLength > MAX_PAYLOAD_SIZE) {
      throw new Error(
        `frame: payload ${payloadLength} bytes exceeds MAX_PAYLOAD_SIZE (${MAX_PAYLOAD_SIZE})`,
      );
    }
    const need = HEADER_SIZE + payloadLength;
    if (remaining < need) {
      throw new Error(
        `frame: buffer too short for declared payload (need ${need} bytes, have ${remaining})`,
      );
    }
    frames.push({
      type,
      streamId,
      payload: data.subarray(offset + HEADER_SIZE, offset + need) as Buffer,
    });
    offset += need;
  }
  return frames;
}
