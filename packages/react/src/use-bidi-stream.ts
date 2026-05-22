import { useCallback, useEffect, useRef, useState } from "react";
import type { BidiStream } from "@fugue-rpc/transport";
import { GrpcStatusError } from "@fugue-rpc/transport";

export type BidiStreamState<Res> =
  | { status: "idle" }
  | { status: "open"; messages: Res[] }
  | { status: "done"; messages: Res[] }
  | { status: "error"; messages: Res[]; error: GrpcStatusError | Error };

export interface UseBidiStreamResult<Req, Res> {
  state: BidiStreamState<Res>;
  /**
   * Open the stream. No-ops if the stream is already open — call `cancel()` or
   * `reset()` first if you need to restart. Optionally send an initial request
   * immediately after opening (useful for subscribe/auth patterns).
   */
  open(initialRequest?: Req): void;
  send(req: Req): void;
  halfClose(): void;
  cancel(): void;
  reset(): void;
}

/**
 * Manages a bidirectional-streaming RPC.
 *
 * @param call - Factory that opens a bidi stream (no request argument).
 *
 * @remarks Each incoming message triggers a React state update with a shallow
 * copy of the accumulated messages array. This is O(n) per message and O(n²)
 * in total copy operations over the lifetime of a stream. For streams
 * delivering many messages at high frequency, consume `transport.openStream()`
 * directly and manage state yourself.
 */
export function useBidiStream<Req, Res>(
  call: () => BidiStream<Req, Res>,
): UseBidiStreamResult<Req, Res> {
  const [state, setState] = useState<BidiStreamState<Res>>({ status: "idle" });
  const streamRef = useRef<BidiStream<Req, Res> | null>(null);
  const cancelledRef = useRef(false);

  const cancel = useCallback(() => {
    if (streamRef.current) {
      streamRef.current.cancel();
      streamRef.current = null;
    }
    cancelledRef.current = true;
  }, []);

  const open = useCallback(
    (initialRequest?: Req) => {
      // Guard: no-op when already open. Caller must cancel() or reset() first.
      if (streamRef.current !== null) return;

      cancelledRef.current = false;
      const stream = call();
      streamRef.current = stream;
      if (initialRequest !== undefined) stream.send(initialRequest);
      setState({ status: "open", messages: [] });

      (async () => {
        const msgs: Res[] = [];
        try {
          for await (const msg of stream) {
            if (cancelledRef.current) return;
            msgs.push(msg);
            setState({ status: "open", messages: [...msgs] });
          }
          if (!cancelledRef.current) {
            streamRef.current = null;
            setState({ status: "done", messages: msgs });
          }
        } catch (err: unknown) {
          if (!cancelledRef.current) {
            streamRef.current = null;
            setState({
              status: "error",
              messages: msgs,
              error: err instanceof Error ? err : new Error(String(err)),
            });
          }
        }
      })();
    },
    [call],
  );

  const send = useCallback((req: Req) => {
    streamRef.current?.send(req);
  }, []);

  const halfClose = useCallback(() => {
    streamRef.current?.halfClose();
  }, []);

  const reset = useCallback(() => {
    cancel();
    setState({ status: "idle" });
  }, [cancel]);

  useEffect(() => {
    return () => {
      cancel();
    };
  }, [cancel]);

  return { state, open, send, halfClose, cancel, reset };
}
