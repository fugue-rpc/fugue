import { useCallback, useEffect, useRef, useState } from "react";
import type { ServerStream } from "@grpcws/transport";
import { GrpcStatusError } from "@grpcws/transport";

export type ServerStreamState<Res> =
  | { status: "idle" }
  | { status: "streaming"; messages: Res[] }
  | { status: "done"; messages: Res[] }
  | { status: "error"; messages: Res[]; error: GrpcStatusError | Error };

export interface UseServerStreamResult<Req, Res> {
  state: ServerStreamState<Res>;
  start(req: Req): void;
  cancel(): void;
  reset(): void;
}

/**
 * Manages a server-streaming RPC.
 *
 * @param call - Factory that opens a server stream given a request.
 *
 * @remarks Each incoming message triggers a React state update with a shallow
 * copy of the accumulated messages array. This is O(n) per message and O(n²)
 * in total copy operations over the lifetime of a stream. For streams
 * delivering many messages at high frequency, consume `transport.openStream()`
 * directly and manage state yourself.
 */
export function useServerStream<Req, Res>(
  call: (req: Req) => ServerStream<Res>,
): UseServerStreamResult<Req, Res> {
  const [state, setState] = useState<ServerStreamState<Res>>({ status: "idle" });
  const streamRef = useRef<ServerStream<Res> | null>(null);
  const cancelledRef = useRef(false);

  const cancel = useCallback(() => {
    if (streamRef.current) {
      streamRef.current.cancel();
      streamRef.current = null;
    }
    cancelledRef.current = true;
  }, []);

  const start = useCallback(
    (req: Req) => {
      cancel();
      cancelledRef.current = false;
      const stream = call(req);
      streamRef.current = stream;
      setState({ status: "streaming", messages: [] });

      (async () => {
        const msgs: Res[] = [];
        try {
          for await (const msg of stream) {
            if (cancelledRef.current) return;
            msgs.push(msg);
            setState({ status: "streaming", messages: [...msgs] });
          }
          if (!cancelledRef.current) {
            setState({ status: "done", messages: msgs });
          }
        } catch (err: unknown) {
          if (!cancelledRef.current) {
            setState({
              status: "error",
              messages: msgs,
              error: err instanceof Error ? err : new Error(String(err)),
            });
          }
        }
      })();
    },
    [call, cancel],
  );

  const reset = useCallback(() => {
    cancel();
    setState({ status: "idle" });
  }, [cancel]);

  useEffect(() => {
    return () => {
      cancel();
    };
  }, [cancel]);

  return { state, start, cancel, reset };
}
