// Proto schemas for fugue frame payloads.
// Copied from gen/ts/grpcws/frame/v1/frame_pb.ts so node-server has no
// dependency on the gen/ directory outside the package.
/* eslint-disable */

import type { GenFile, GenMessage } from "@bufbuild/protobuf/codegenv2";
import { fileDesc, messageDesc } from "@bufbuild/protobuf/codegenv2";
import type { Message } from "@bufbuild/protobuf";

export const file_grpcws_frame_v1_frame: GenFile = /*@__PURE__*/
  fileDesc("ChtncnBjd3MvZnJhbWUvdjEvZnJhbWUucHJvdG8SD2dycGN3cy5mcmFtZS52MSKOAQoMQmVnaW5QYXlsb2FkEg4KBm1ldGhvZBgBIAEoCRI9CghtZXRhZGF0YRgCIAMoCzIrLmdycGN3cy5mcmFtZS52MS5CZWdpblBheWxvYWQuTWV0YWRhdGFFbnRyeRovCg1NZXRhZGF0YUVudHJ5EgsKA2tleRgBIAEoCRINCgV2YWx1ZRgCIAEoCToCOAEifQoNSGVhZGVyUGF5bG9hZBI8CgdoZWFkZXJzGAEgAygLMisuZ3JwY3dzLmZyYW1lLnYxLkhlYWRlclBheWxvYWQuSGVhZGVyc0VudHJ5Gi4KDEhlYWRlcnNFbnRyeRILCgNrZXkYASABKAkSDQoFdmFsdWUYAiABKAk6AjgBIqcBCgpFbmRQYXlsb2FkEhMKC3N0YXR1c19jb2RlGAEgASgNEhYKDnN0YXR1c19tZXNzYWdlGAIgASgJEjsKCHRyYWlsZXJzGAMgAygLMikuZ3JwY3dzLmZyYW1lLnYxLkVuZFBheWxvYWQuVHJhaWxlcnNFbnRyeRovCg1UcmFpbGVyc0VudHJ5EgsKA2tleRgBIAEoCRINCgV2YWx1ZRgCIAEoCToCOAFCK1opZ2l0aHViLmNvbS9ncnBjd3Mvd3NncnBjL2ZyYW1lL3YxO2ZyYW1ldjFiBnByb3RvMw");

export type BeginPayload = Message<"grpcws.frame.v1.BeginPayload"> & {
  method: string;
  metadata: { [key: string]: string };
};
export const BeginPayloadSchema: GenMessage<BeginPayload> = /*@__PURE__*/
  messageDesc(file_grpcws_frame_v1_frame, 0);

export type HeaderPayload = Message<"grpcws.frame.v1.HeaderPayload"> & {
  headers: { [key: string]: string };
};
export const HeaderPayloadSchema: GenMessage<HeaderPayload> = /*@__PURE__*/
  messageDesc(file_grpcws_frame_v1_frame, 1);

export type EndPayload = Message<"grpcws.frame.v1.EndPayload"> & {
  statusCode: number;
  statusMessage: string;
  trailers: { [key: string]: string };
};
export const EndPayloadSchema: GenMessage<EndPayload> = /*@__PURE__*/
  messageDesc(file_grpcws_frame_v1_frame, 2);
