// Node.js echo server — demonstrates @fugue-rpc/node-server with all four RPC kinds.
// Mirrors examples/echo-server (Go): Echo, EchoStream, EchoCollect, EchoBidi, EchoStreamN.
//
// Codecs:
//   Msg { string value = 1 }       — tag 0x0a + 1-byte len + UTF-8 (values ≤ 127 bytes)
//   StreamNReq { string value = 1; int32 count = 2 } — full varint proto decode

import { createServer } from "node:http";
import {
  FugueServer,
  type ServiceDefinition,
  type UnaryHandler,
  type ServerStreamHandler,
  type ClientStreamHandler,
  type BidiHandler,
} from "@fugue-rpc/node-server";

function encodeMsg(value: string): Buffer {
  const data = Buffer.from(value, "utf8");
  const out = Buffer.allocUnsafe(2 + data.length);
  out[0] = 0x0a;
  out[1] = data.length;
  data.copy(out, 2);
  return out;
}

function decodeMsg(buf: Buffer): string {
  return buf.subarray(2).toString("utf8");
}

// Decode StreamNReq { string value = 1; int32 count = 2 } using a minimal varint loop.
function decodeStreamNReq(buf: Buffer): { value: string; count: number } {
  let pos = 0;
  let value = "";
  let count = 1;
  while (pos < buf.length) {
    let tag = 0, shift = 0;
    while (pos < buf.length) { const b = buf[pos++]; tag |= (b & 0x7f) << shift; if (!(b & 0x80)) break; shift += 7; }
    const wireType = tag & 0x7;
    const fieldNum = tag >>> 3;
    if (wireType === 2) {
      let len = 0; shift = 0;
      while (pos < buf.length) { const b = buf[pos++]; len |= (b & 0x7f) << shift; if (!(b & 0x80)) break; shift += 7; }
      if (fieldNum === 1) value = buf.subarray(pos, pos + len).toString("utf8");
      pos += len;
    } else if (wireType === 0) {
      let v = 0; shift = 0;
      while (pos < buf.length) { const b = buf[pos++]; v |= (b & 0x7f) << shift; if (!(b & 0x80)) break; shift += 7; }
      if (fieldNum === 2) count = v;
    } else { break; }
  }
  return { value, count };
}

const EchoService = {
  echo:         { path: "/echo.v1.Echo/Echo",         requestStream: false, responseStream: false, requestDeserialize: decodeMsg,        responseSerialize: encodeMsg },
  echoStream:   { path: "/echo.v1.Echo/EchoStream",   requestStream: false, responseStream: true,  requestDeserialize: decodeMsg,        responseSerialize: encodeMsg },
  echoCollect:  { path: "/echo.v1.Echo/EchoCollect",  requestStream: true,  responseStream: false, requestDeserialize: decodeMsg,        responseSerialize: encodeMsg },
  echoBidi:     { path: "/echo.v1.Echo/EchoBidi",     requestStream: true,  responseStream: true,  requestDeserialize: decodeMsg,        responseSerialize: encodeMsg },
  echoStreamN:  { path: "/echo.v1.Echo/EchoStreamN",  requestStream: false, responseStream: true,  requestDeserialize: decodeStreamNReq, responseSerialize: encodeMsg },
} satisfies ServiceDefinition;

const srv = new FugueServer({ origins: "*" });
srv.addService(EchoService, {
  echo: (async (call) => call.request) satisfies UnaryHandler<string, string>,

  echoStream: (async (call) => {
    for (let i = 0; i < 5; i++) call.write(call.request);
  }) satisfies ServerStreamHandler<string, string>,

  echoCollect: (async (call) => {
    const parts: string[] = [];
    for await (const v of call) parts.push(v);
    return parts.join(",");
  }) satisfies ClientStreamHandler<string, string>,

  echoBidi: (async (call) => {
    for await (const v of call) call.write(v);
  }) satisfies BidiHandler<string, string>,

  echoStreamN: (async (call) => {
    for (let i = 0; i < call.request.count; i++) call.write(call.request.value);
  }) satisfies ServerStreamHandler<{ value: string; count: number }, string>,
});

const PORT = Number(process.env.PORT ?? 8080);
const httpServer = createServer();
srv.attach(httpServer, "/fugue/");

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log(`node-echo-server listening :${PORT}/fugue/`);
});

async function shutdown() {
  await srv.close();
  httpServer.close(() => process.exit(0));
}
process.once("SIGTERM", shutdown);
process.once("SIGINT", shutdown);
