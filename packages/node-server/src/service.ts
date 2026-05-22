// gRPC service definition and handler interfaces for @fugue-rpc/node-server.
//
// ServiceDefinition is duck-compatible with the output of @grpc/grpc-js's
// protoc-gen-grpc-js plugin so users can pass their existing generated service
// descriptor objects directly. We define our own interface to avoid a runtime
// dependency on @grpc/grpc-js.

// ---- Method & service definition ----

/**
 * Per-method descriptor. Structurally compatible with @grpc/grpc-js's
 * MethodDefinition type. The server only uses requestDeserialize and
 * responseSerialize; any client-side fields (requestSerialize,
 * responseDeserialize) are accepted but ignored.
 */
export interface MethodDefinition<Req, Res> {
  readonly path: string;
  readonly requestStream: boolean;
  readonly responseStream: boolean;
  readonly requestDeserialize: (buf: Buffer) => Req;
  readonly responseSerialize: (value: Res) => Buffer;
  // Fields present in @grpc/grpc-js output; ignored on the server side.
  readonly requestSerialize?: (value: Req) => Buffer;
  readonly responseDeserialize?: (buf: Buffer) => Res;
}

// `any` is intentional: each method's Req/Res types are recovered at the
// method level when building a MethodEntry; the registry itself is untyped.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ServiceDefinition = { readonly [method: string]: MethodDefinition<any, any> };

// Using `(call: any) => any` rather than `AnyHandler` (a union of typed function
// types) because TypeScript cannot contextually type function parameters from a
// union — it can't pick which variant applies. The concrete shape lets users
// write inline handlers without explicit annotations. Typed handler constants
// (UnaryHandler<Req,Res> etc.) remain the right choice for full type safety.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ServiceImplementation = Record<string, (call: any) => any>;

// ---- RPC kind ----

export type RpcKind = "unary" | "server_stream" | "client_stream" | "bidi_stream";

export function rpcKindOf(requestStream: boolean, responseStream: boolean): RpcKind {
  if (!requestStream && !responseStream) return "unary";
  if (!requestStream && responseStream) return "server_stream";
  if (requestStream && !responseStream) return "client_stream";
  return "bidi_stream";
}

// ---- Call objects seen by handlers ----

/** Metadata and header/trailer control shared by all call shapes. */
export interface CallBase {
  readonly metadata: Record<string, string>;
  sendHeader(header: Record<string, string>): void;
  setTrailer(trailer: Record<string, string>): void;
}

/** Unary: single request → handler returns single response. */
export interface UnaryServerCall<Req> extends CallBase {
  readonly request: Req;
}

/** Server-streaming: single request → handler calls write() N times, then returns. */
export interface ServerStreamCall<Req, Res> extends CallBase {
  readonly request: Req;
  write(response: Res): void;
}

/**
 * Client-streaming: handler iterates N incoming requests via async iteration,
 * then returns one response.
 */
export interface ClientStreamCall<Req> extends CallBase, AsyncIterable<Req> {}

/**
 * Bidirectional: handler iterates incoming requests and calls write()
 * independently — both directions are open until the handler returns.
 */
export interface BidiCall<Req, Res> extends CallBase, AsyncIterable<Req> {
  write(response: Res): void;
}

// ---- Handler types ----

export type UnaryHandler<Req, Res> =
  (call: UnaryServerCall<Req>) => Res | Promise<Res>;

export type ServerStreamHandler<Req, Res> =
  (call: ServerStreamCall<Req, Res>) => Promise<void>;

export type ClientStreamHandler<Req, Res> =
  (call: ClientStreamCall<Req>) => Res | Promise<Res>;

export type BidiHandler<Req, Res> =
  (call: BidiCall<Req, Res>) => Promise<void>;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyHandler =
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  | UnaryHandler<any, any>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  | ServerStreamHandler<any, any>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  | ClientStreamHandler<any, any>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  | BidiHandler<any, any>;

// ---- Internal registry record ----

/**
 * What the connection layer looks up by method path. Bundles the handler with
 * the codec functions from the service definition so conn.ts only needs one
 * Map lookup per incoming stream.
 */
export interface MethodEntry {
  readonly path: string;
  readonly kind: RpcKind;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly deserialize: (buf: Buffer) => any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly serialize: (val: any) => Buffer;
  readonly handler: AnyHandler;
}

// ---- ServiceRegistry ----

export class ServiceRegistry {
  private readonly _methods = new Map<string, MethodEntry>();

  /**
   * Register all methods from `definition` with handlers from `implementation`.
   *
   * - Keys in `implementation` with no matching method in `definition` are
   *   silently ignored.
   * - Methods in `definition` with no handler are not registered; callers
   *   will receive UNIMPLEMENTED.
   *
   * Structurally compatible with grpc.Server.addService from @grpc/grpc-js.
   */
  addService(
    definition: ServiceDefinition,
    implementation: ServiceImplementation,
  ): void {
    for (const [methodName, methodDef] of Object.entries(definition)) {
      const handler = implementation[methodName];
      if (typeof handler !== "function") continue;

      this._methods.set(methodDef.path, {
        path: methodDef.path,
        kind: rpcKindOf(methodDef.requestStream, methodDef.responseStream),
        deserialize: methodDef.requestDeserialize,
        serialize: methodDef.responseSerialize,
        handler,
      });
    }
  }

  /** Returns the MethodEntry for `path`, or undefined if not registered. */
  lookup(path: string): MethodEntry | undefined {
    return this._methods.get(path);
  }

  /** All registered method paths — useful for diagnostics and startup logging. */
  registeredPaths(): string[] {
    return [...this._methods.keys()];
  }
}
