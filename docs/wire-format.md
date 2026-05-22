# fugue Wire Format Specification — v0.1

This document is the authoritative specification for the fugue binary framing protocol. An independent implementer should be able to build a compatible client from this document alone without reading any library source code.

---

## 1. Transport

fugue runs over a single, long-lived WebSocket connection (`ws://` or `wss://`). All messages are **binary WebSocket frames** (`opcode 0x2`). Text frames are a protocol error.

The client opens the WebSocket to a path of the server's choosing (conventionally `/fugue/`). Multiple gRPC streams are multiplexed over the single connection using stream IDs assigned by the client.

---

## 2. Frame Layout

Every message is a **fugue frame** consisting of a fixed 9-byte header followed by a variable-length payload.

```
 Byte:  0        1        2        3        4        5        6        7        8
       +--------+--------+--------+--------+--------+--------+--------+--------+--------+
       |  type  |         stream_id (uint32, big-endian)       |      payload_length (uint32, big-endian)      |
       +--------+--------+--------+--------+--------+--------+--------+--------+--------+
       |                            payload  (0 .. payload_length bytes)                 ...
       +------------------------------------------------------------------------------------
```

| Field            | Bytes | Type              | Description                              |
|------------------|-------|-------------------|------------------------------------------|
| `type`           | 1     | uint8             | Frame type (see §3)                      |
| `stream_id`      | 4     | uint32, big-endian | Stream this frame belongs to (see §5)   |
| `payload_length` | 4     | uint32, big-endian | Byte length of the payload that follows |
| `payload`        | var   | bytes             | Frame-type-specific content (see §4)    |

Total header size: **9 bytes**.

### Byte order

All multi-byte integers use **network byte order (big-endian)**. This is the default for `encoding/binary.BigEndian` in Go and the `false` (littleEndian=false) path in the browser `DataView` API.

**Reference parsing — TypeScript:**

```typescript
// buffer: ArrayBuffer containing at least 9 bytes at offset 0
const view = new DataView(buffer);
const frameType     = view.getUint8(0);
const streamId      = view.getUint32(1, false);   // false = big-endian
const payloadLength = view.getUint32(5, false);   // false = big-endian
const payload       = buffer.slice(9, 9 + payloadLength);
```

**Reference parsing — Go:**

```go
import "encoding/binary"

// header is a [9]byte read from the connection
frameType  := header[0]
streamID   := binary.BigEndian.Uint32(header[1:5])
payloadLen := binary.BigEndian.Uint32(header[5:9])
// payload: read the next payloadLen bytes
```

### MAX_PAYLOAD_SIZE

```
MAX_PAYLOAD_SIZE = 4 * 1024 * 1024   // 4 MiB = 4,194,304 bytes
```

A frame with `payload_length > MAX_PAYLOAD_SIZE` is a protocol error. The receiver MUST send a RESET frame for that stream with gRPC status `RESOURCE_EXHAUSTED` (code 8) and then close the WebSocket connection with close code 1002.

---

## 3. Frame Types

| Constant | Value  | Direction        | Description                                        |
|----------|--------|------------------|----------------------------------------------------|
| `BEGIN`  | `0x01` | client → server  | Open a new stream and invoke a gRPC method         |
| `MSG`    | `0x02` | client ↔ server  | Carry one serialized protobuf request or response  |
| `END`    | `0x03` | client ↔ server  | Half-close or fully close a stream with gRPC status|
| `RESET`  | `0x04` | client ↔ server  | Abort a stream immediately                         |
| —        | `0x05` | —                | **Reserved.** v0.1 receivers MUST treat as a protocol error. Reserved for future application-layer PING/keepalive. |
| `HEADER` | `0x06` | server → client  | Server response headers (initial metadata), sent before the first MSG |

Frame type values not listed above (`0x00`, `0x07`–`0xFF`) are undefined. Receiving an undefined frame type is a protocol error.

---

## 4. Frame Definitions

### 4.1 BEGIN

**Direction:** client → server  
**Payload:** serialized `BeginPayload` proto message (see §6)

Sent by the client to open a new stream. `stream_id` MUST be greater than any stream ID previously used on this connection (see §5). The `method` field in `BeginPayload` MUST be the full gRPC method path in the format `/package.ServiceName/MethodName`.

A server that receives a BEGIN for an already-open stream MUST treat it as a protocol error and close the WebSocket.

### 4.2 MSG

**Direction:** client ↔ server  
**Payload:** a serialized protobuf message

Carries one message in the stream. The message type is determined by the gRPC method definition conveyed in the originating BEGIN frame. No type tag is embedded in the MSG frame.

- Client sends MSG frames to deliver request messages (for client-streaming and bidi RPC types).
- Server sends MSG frames to deliver response messages (for server-streaming and bidi RPC types).
- For unary RPCs: the client sends exactly one MSG then sends END; the server sends exactly one MSG then sends END.
- MSG frames MUST NOT be sent after an END or RESET has been sent on that stream in the same direction.

### 4.3 END

**Direction:** client ↔ server  
**Payload:** serialized `EndPayload` proto message (see §6)

Signals the end of messages in one direction (half-close) or the terminal close of the stream.

**Client-sent END** (half-close):
- Signals that the client will send no more MSG frames on this stream.
- `status_code` SHOULD be `0` (OK); `status_message` and `trailers` SHOULD be empty.
- Valid only in states `OPEN` or `HALF_CLOSED_REMOTE`.

**Server-sent END** (stream close):
- Carries the final gRPC status and trailing metadata.
- `status_code` is the gRPC status code (`0` = OK).
- `trailers` contains any trailing metadata the handler set.
- After sending END, the server considers the stream closed.

### 4.4 RESET

**Direction:** client ↔ server  
**Payload:** serialized `EndPayload` proto message, OR empty

Aborts a stream immediately, regardless of current state. No further frames should be sent for that stream after RESET.

- If payload is empty, the abort reason is treated as `CANCELLED` (gRPC code 1).
- If payload is non-empty, it is a serialized `EndPayload` carrying the abort status.
- Receiving a RESET for an **unknown** stream ID (stream already closed or never opened): **silently drop it**. This is not a protocol error — it occurs in normal operation when END and RESET cross in flight.
- Receiving a RESET for a known open stream: cancel the stream, free all resources.

### 4.5 HEADER

**Direction:** server → client  
**Payload:** serialized `HeaderPayload` proto message (see §6)

Carries server response headers (initial metadata) before the first MSG frame. Sending a HEADER frame is optional — if the server handler never calls `SendHeader` or `SetHeader`, no HEADER frame is sent and the client treats initial metadata as empty.

Rules:
- A server MUST send at most one HEADER frame per stream.
- A HEADER frame MUST be sent before any MSG frame in the server → client direction. Sending HEADER after MSG is a protocol error.
- If the server calls `SendMsg` without first calling `SendHeader`, the implementation MUST auto-flush an empty HEADER frame before sending the MSG frame.
- The client MUST be prepared to receive a HEADER frame at any point before the first server MSG.

### 4.7 Type 0x05 — Reserved

Receiving a frame with type `0x05` is a protocol error in v0.1. Close the WebSocket with code 1002.

---

## 5. Stream Lifecycle

### 5.1 Stream ID Assignment

- **The client assigns all stream IDs.** The server never initiates a stream in v0.1.
- **Stream ID 0 is reserved** for future connection-level signaling. Receiving any frame with `stream_id = 0` is a protocol error.
- **Stream IDs start at 1** and MUST be **monotonically increasing** per connection. The server tracks the highest stream ID seen. Receiving a BEGIN with `stream_id ≤ highest_seen` is a protocol error — close the WebSocket with code 1002.
- **ID space exhaustion:** if a client reaches `stream_id = 0xFFFFFFFF` (4,294,967,295), it MUST close the transport and open a new WebSocket connection. In practice this is unreachable in any normal workload.

### 5.2 Stream State Machine

States are tracked independently **per direction** (send side / receive side), but the abbreviated combined-state machine below is sufficient for v0.1 where the client owns MSG-send and the server owns MSG-receive on the client side.

```
CLIENT VIEW
===========

  [client sends BEGIN]
         |
         v
       OPEN
      /    \
     /      \
    /        \
   v          v
client       server
sends END    sends END
(half-close) (half-close)
   |              |
   v              v
HALF_CLOSED   HALF_CLOSED
_LOCAL        _REMOTE
(can recv,    (can send,
 no more       no more
 client MSG)   server MSG)
   |              |
   | server       | client
   | sends END    | sends END
   |              | (half-close)
   v              v
         CLOSED
           ^
           |
    RESET received or sent
    (from any state above)
```

**Formal state transitions:**

| Current State       | Event                          | Next State          | Action                          |
|---------------------|--------------------------------|---------------------|---------------------------------|
| `OPEN`              | client sends END               | `HALF_CLOSED_LOCAL` | no more client MSG allowed      |
| `OPEN`              | server sends END               | `HALF_CLOSED_REMOTE`| no more server MSG expected     |
| `OPEN`              | RESET sent or received         | `CLOSED`            | free stream resources           |
| `HALF_CLOSED_LOCAL` | server sends END               | `CLOSED`            | stream fully done               |
| `HALF_CLOSED_LOCAL` | RESET sent or received         | `CLOSED`            | free stream resources           |
| `HALF_CLOSED_REMOTE`| client sends END               | `CLOSED`            | stream fully done               |
| `HALF_CLOSED_REMOTE`| RESET sent or received         | `CLOSED`            | free stream resources           |
| `CLOSED`            | any frame arrives for this ID  | —                   | silently drop (MSG/END/RESET are all in-flight races, not errors) |

### 5.3 Per-RPC-Type Frame Sequences

**Unary** (`SayHello`):
```
Client: BEGIN  MSG  END
Server:              MSG  END
```

**Server-streaming** (`ListMessages`):
```
Client: BEGIN  MSG  END
Server:              MSG  MSG  MSG  END
```

**Client-streaming** (`UploadChunks`):
```
Client: BEGIN  MSG  MSG  MSG  END
Server:                        MSG  END
```

**Bidirectional streaming** (`Chat`):
```
Client: BEGIN  MSG  MSG  END
Server:         MSG  MSG  MSG  END
```
(MSG frames from client and server may interleave freely.)

---

## 6. Proto Definitions

These message types are defined in `proto/grpcws/frame/v1/frame.proto`.

```proto
syntax = "proto3";

package grpcws.frame.v1;

option go_package = "github.com/fugue-rpc/fugue/frame/v1;framev1";

// Payload for a BEGIN frame. Sent by the client to open a stream.
message BeginPayload {
  // Full gRPC method path: "/package.ServiceName/MethodName"
  string method = 1;

  // Request metadata (equivalent to gRPC request headers).
  // Keys are lowercase. Binary metadata keys end in "-bin" and have
  // base64-encoded values.
  map<string, string> metadata = 2;
}

// Payload for a HEADER frame. Sent by the server before the first MSG.
message HeaderPayload {
  // Server initial metadata (response headers).
  // Keys follow the same conventions as BeginPayload.metadata.
  map<string, string> headers = 1;
}

// Payload for an END frame and optionally a RESET frame.
message EndPayload {
  // gRPC status code. 0 = OK. See https://grpc.github.io/grpc/core/md_doc_statuscodes.html
  uint32 status_code = 1;

  // Human-readable status message. Empty on success.
  string status_message = 2;

  // Trailing metadata from the server handler.
  // Keys follow the same conventions as BeginPayload.metadata.
  map<string, string> trailers = 3;
}
```

---

## 7. Connection Setup and Origin Checking

### WebSocket Upgrade

The client initiates a standard WebSocket upgrade (`Upgrade: websocket`) to the server's endpoint. The server upgrades on `101 Switching Protocols` or rejects per the rules below.

### Origin Enforcement

The server enforces origin checking at upgrade time using the `Origin` request header. This is distinct from CORS: WebSocket upgrades are **exempt from CORS preflight** by the Fetch specification, so the server does not set `Access-Control-*` response headers. The origin check happens at the HTTP upgrade, not at a preflight.

**Rules:**

| Server configuration        | `Origin` header present?  | Result                       |
|-----------------------------|---------------------------|------------------------------|
| `NewServer()` — no option   | yes, any origin           | `403 Forbidden`, no upgrade  |
| `NewServer()` — no option   | absent (non-browser)      | `101`, upgrade proceeds      |
| `WithOrigins("https://a.example.com")` | matches allow-list | `101`, upgrade proceeds |
| `WithOrigins("https://a.example.com")` | does not match    | `403 Forbidden`, no upgrade  |
| `WithOrigins("*")`          | any / absent              | `101`, upgrade proceeds      |

`WithOrigins("*")` is **development-only**. When a logger is configured via `WithLogger`, the server logs a warning at startup:

```
fugue: WithOrigins("*") — all origins permitted; do not use in production
```

The absent-Origin rule (non-browser clients pass unconditionally) exists because the `Origin` header is a browser security mechanism. Non-browser clients — Go test programs, CLI tools, native apps — do not send it and should not be blocked.

---

## 8. Protocol Error Reference

A **protocol error** is any violation of this specification. When a protocol error is detected the receiver MUST close the WebSocket (code 1002 for connection-level errors, or send RESET + close for stream-level errors) and free all associated resources.

| Violation                                          | Error level    | Response                                  |
|----------------------------------------------------|----------------|-------------------------------------------|
| `stream_id = 0` in any frame                       | Connection     | Close WebSocket code 1002                 |
| BEGIN with `stream_id ≤ highest_seen`              | Connection     | Close WebSocket code 1002                 |
| `payload_length > MAX_PAYLOAD_SIZE`                | Stream         | RESET (RESOURCE_EXHAUSTED), close WS 1002 |
| Undefined frame type                               | Connection     | Close WebSocket code 1002                 |
| Frame type `0x05`                                  | Connection     | Close WebSocket code 1002                 |
| Binary WebSocket frame with `payload_length` inconsistent with WebSocket frame length | Connection | Close WebSocket code 1002 |
| Text WebSocket frame                               | Connection     | Close WebSocket code 1002                 |
| MSG or END for unknown (closed/unseen) stream ID   | —              | Silently dropped (same in-flight race as RESET; not a protocol error) |
| BEGIN for an already-open stream ID                | Connection     | Close WebSocket code 1002                 |
| MSG sent after END in the same direction           | Stream         | RESET (INTERNAL), close WS 1002           |
| HEADER sent after MSG in the server → client direction | Stream     | RESET (INTERNAL), close WS 1002           |
| More than one HEADER frame sent on a stream        | Stream         | RESET (INTERNAL), close WS 1002           |

**Exception:** RESET, MSG, and END received for an unknown or already-closed stream ID are silently dropped. These are not protocol errors — they occur in normal operation when frames cross in flight (e.g. a MSG sent by the client before it received the server's END). Closing the connection in response would kill all other live streams on the connection for no reason. See §4.4.

---

## 9. gRPC Status Codes

The `status_code` field in `EndPayload` and `ResetPayload` uses standard gRPC status codes:

| Code | Name                | Meaning in this context                        |
|------|---------------------|------------------------------------------------|
| 0    | OK                  | Success                                        |
| 1    | CANCELLED           | Stream cancelled by client or server           |
| 2    | UNKNOWN             | Unspecified error                              |
| 4    | DEADLINE_EXCEEDED   | Handler timeout                                |
| 5    | NOT_FOUND           | Method not registered on the server            |
| 8    | RESOURCE_EXHAUSTED  | Payload exceeded MAX_PAYLOAD_SIZE              |
| 12   | UNIMPLEMENTED       | Method exists in proto but not registered      |
| 13   | INTERNAL            | Server-side unexpected error                   |

Full list: https://grpc.github.io/grpc/core/md_doc_statuscodes.html

---

## 10. Version and Compatibility

This document describes **fugue wire format v0.1**.

- The frame header has no version field. Versioning is handled at the WebSocket path level (e.g. `/fugue/v1/`).
- Frame type `0x05` and all values above are reserved. A v0.1 implementation MUST reject them as protocol errors so future versions can be detected cleanly.
- The proto field numbers in `BeginPayload` and `EndPayload` are stable. Future versions may add fields; unknown fields MUST be ignored per proto3 semantics.
